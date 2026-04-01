// src/db.rs

use memmap2::MmapMut;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{compiler_fence, Ordering};
use std::sync::RwLock;

use crate::u1024::U1024;

pub const DEFAULT_CLUSTER_DIR: &str = "data/thrush_cluster";

// ─────────────────────────────────────────────────────────────────────────────
// TWO-LEVEL LSH DESIGN
//
// Level 1 — Cluster Router (which chunk?)
//   Uses K=4 bit sampling → 16 buckets → % num_chunks
//   3 independent hash tables → 97.8% recall at D=80
//
// Level 2 — Inner Chunk Router (which slot in the chunk?)
//   Uses K=10 bit sampling → 1024 buckets → each bucket covers ~977 slots
//   8 independent projections → 99.5% recall at D=80
//
// WHY K=10 INNER:
//   P(10 sampled bits all agree | D=80) = (1 - 80/1024)^10 = 0.9219^10 ≈ 0.44
//   8 projections: 1 - (1-0.44)^8 ≈ 99.5% recall
//
// WHY THE ORIGINAL lsh_indices FAILED:
//   It extracted raw 20-bit slices from specific word positions and XOR'd them.
//   Two similar vectors (D=80) have ~8% bit flip rate. A raw 20-bit slice has
//   P(all 20 bits identical) = (0.922)^20 ≈ 0.196 per projection.
//   8 projections: 1-(1-0.196)^8 ≈ 85% — barely acceptable.
//   BUT: it then did % capacity (1,000,000), meaning the 20-bit value (0..1M)
//   directly became the slot. Even a single bit flip anywhere in those 20 bits
//   jumps to a completely different slot. probe_depth=64 can't bridge that gap.
//   Effective recall: near 0% with sparse data.
// ─────────────────────────────────────────────────────────────────────────────

// ── Cluster-level (K=4, 3 tables) ────────────────────────────────────────────

const NUM_TABLES: usize = 3;

// (word_index, bit_shift) — extracts one bit. All shifts in [0, 63].
const CLUSTER_HASH_BITS: [[(usize, u64); 4]; NUM_TABLES] = [
    [(0, 17), (4, 43), (9,  7), (13, 55)],
    [(2, 29), (6, 51), (11, 3), (15, 37)],
    [(1, 61), (7, 13), (10,41), (14, 23)],
];

#[inline(always)]
fn cluster_table_hash(vector: &U1024, table: usize, num_chunks: usize) -> usize {
    let b = &CLUSTER_HASH_BITS[table];
    let h = ((vector.0[b[0].0] >> b[0].1) & 1)
          | (((vector.0[b[1].0] >> b[1].1) & 1) << 1)
          | (((vector.0[b[2].0] >> b[2].1) & 1) << 2)
          | (((vector.0[b[3].0] >> b[3].1) & 1) << 3);
    (h as usize) % num_chunks
}

// ── Inner chunk-level (K=10, 8 projections) ──────────────────────────────────

const INNER_PROJECTIONS: usize = 8;
const INNER_K: usize = 10;
const INNER_BUCKETS: usize = 1 << INNER_K; // 1024

// 8 projections × 10 bit positions each.
// Spread across all 16 words, no shift > 63, minimal overlap between projections.
const INNER_HASH_BITS: [[(usize, u64); INNER_K]; INNER_PROJECTIONS] = [
    [(0,3),  (2,7),  (4,11), (6,17), (8,23), (10,29), (12,37), (14,41), (1,53), (3,59)],
    [(1,5),  (3,9),  (5,13), (7,19), (9,27), (11,31), (13,43), (15,47), (0,57), (2,61)],
    [(2,1),  (4,5),  (6,9),  (8,13), (10,19),(12,23), (14,31), (0,37),  (3,47), (5,53)],
    [(3,11), (5,17), (7,21), (9,29), (11,37),(13,43), (15,53), (1,59),  (4,7),  (6,13)],
    [(4,19), (6,23), (8,29), (10,37),(12,41),(14,47), (0,53),  (2,59),  (5,3),  (7,11)],
    [(5,31), (7,37), (9,41), (11,47),(13,53),(15,59), (1,7),   (3,13),  (6,19), (8,29)],
    [(6,43), (8,47), (10,53),(12,59),(14,7), (0,13),  (2,19),  (4,29),  (7,41), (9,53)],
    [(7,51), (9,57), (11,11),(13,17),(15,23),(1,43),  (3,49),  (5,37),  (8,61), (10,7)],
];

#[inline(always)]
fn inner_bucket(vector: &U1024, proj: usize) -> usize {
    let b = &INNER_HASH_BITS[proj];
    let mut h: usize = 0;
    for k in 0..INNER_K {
        let bit = ((vector.0[b[k].0] >> b[k].1) & 1) as usize;
        h |= bit << k;
    }
    h // already in [0, INNER_BUCKETS) = [0, 1024)
}

// ─────────────────────────────────────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VectorRecord {
    pub vector: U1024,     // 128 bytes
    pub payload_id: u64,   // 8 bytes
    pub is_active: u8,     // 1 byte
    pub _pad: [u8; 7],     // 7 bytes — 144 bytes total, cache-line aligned
}

impl Default for VectorRecord {
    fn default() -> Self {
        Self { vector: U1024::ZERO, payload_id: 0, is_active: 0, _pad: [0; 7] }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ThrushCluster
// ─────────────────────────────────────────────────────────────────────────────

pub struct ThrushCluster {
    chunks: Vec<RwLock<ThrushDB>>,
    num_chunks: usize,
}

impl ThrushCluster {
    pub fn new<P: AsRef<Path>>(
        dir: P,
        num_chunks: usize,
        chunk_capacity: usize,
        probe_depth: usize,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let mut chunks = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let path = dir.join(format!("chunk_{}.bin", i));
            let db = ThrushDB::new_at(path, chunk_capacity, probe_depth)?;
            chunks.push(RwLock::new(db));
        }

        Ok(Self { chunks, num_chunks })
    }

    #[inline(always)]
    fn target_chunks(&self, vector: U1024) -> [usize; NUM_TABLES] {
        [
            cluster_table_hash(&vector, 0, self.num_chunks),
            cluster_table_hash(&vector, 1, self.num_chunks),
            cluster_table_hash(&vector, 2, self.num_chunks),
        ]
    }

    pub fn insert(&self, vector: U1024, payload_id: u64) -> Result<(), &'static str> {
        let targets = self.target_chunks(vector);
        let mut any_ok = false;
        for chunk_id in targets {
            let mut guard = self.chunks[chunk_id].write().unwrap();
            if guard.insert(vector, payload_id).is_ok() {
                any_ok = true;
            }
        }
        if any_ok { Ok(()) } else { Err("All LSH neighborhoods full.") }
    }

    pub fn search(&self, query: U1024, k: usize) -> Vec<(u64, u32)> {
        let targets = self.target_chunks(query);
        let mut all_candidates: Vec<(u64, u32)> = Vec::with_capacity(k * NUM_TABLES);
        for chunk_id in targets {
            let guard = self.chunks[chunk_id].read().unwrap();
            all_candidates.append(&mut guard.search(query, k));
        }
        all_candidates.sort_unstable_by_key(|&(_, dist)| dist);
        all_candidates.dedup_by_key(|(id, _)| *id);
        all_candidates.into_iter().take(k).collect()
    }

    pub fn get(&self, payload_id: u64) -> Option<U1024> {
        for chunk_lock in &self.chunks {
            let chunk = chunk_lock.read().unwrap();
            if let Some(vec) = chunk.get(payload_id) {
                return Some(vec);
            }
        }
        None
    }

    pub fn delete(&self, payload_id: u64) -> bool {
        let mut deleted = false;
        for chunk_lock in &self.chunks {
            let mut chunk = chunk_lock.write().unwrap();
            if chunk.delete(payload_id) {
                deleted = true;
                // Don't break — vector lives in up to NUM_TABLES chunks.
            }
        }
        deleted
    }

    pub fn flush(&self) -> io::Result<()> {
        for chunk_lock in &self.chunks {
            chunk_lock.write().unwrap().flush()?;
        }
        Ok(())
    }

    pub fn total_capacity(&self) -> usize {
        self.chunks.iter().map(|c| c.read().unwrap().capacity()).sum()
    }

    pub fn total_len(&self) -> usize {
        self.chunks.iter().map(|c| c.read().unwrap().len()).sum()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ThrushDB (Core Engine)
// ─────────────────────────────────────────────────────────────────────────────

pub struct ThrushDB {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    probe_depth: usize,
}

unsafe impl Send for ThrushDB {}
unsafe impl Sync for ThrushDB {}

impl ThrushDB {
    pub fn new_at<P: AsRef<Path>>(path: P, capacity: usize, probe_depth: usize) -> io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;

        let needed_size = (capacity * std::mem::size_of::<VectorRecord>()) as u64;
        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        if current_size != needed_size {
            file.set_len(needed_size)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // Cache warmup: pull full mmap into RAM to avoid page-fault thrashing.
        let mmap_ptr = mmap.as_ptr();
        let mut _dummy = 0u8;
        for i in (0..mmap.len()).step_by(4096) {
            unsafe { _dummy ^= std::ptr::read_volatile(mmap_ptr.add(i)); }
        }

        Ok(Self { _file: file, mmap, capacity, probe_depth })
    }

    #[inline]
    pub(crate) fn arena(&self) -> &[VectorRecord] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const VectorRecord, self.capacity) }
    }

    #[inline]
    pub(crate) fn arena_mut(&mut self) -> &mut [VectorRecord] {
        unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr() as *mut VectorRecord, self.capacity) }
    }

    pub fn flush(&mut self) -> io::Result<()> { self.mmap.flush() }

    // Returns INNER_PROJECTIONS starting slot indices within this chunk's arena.
    // Each index is the head of a bucket. Linear probing extends probe_depth slots.
    // Two similar vectors land in the same bucket with P ≈ 0.44 per projection.
    // Across 8 projections: P(at least one matches) ≈ 99.5%.
    #[inline(always)]
    pub fn lsh_indices(&self, vector: U1024) -> [usize; INNER_PROJECTIONS] {
        let slots_per_bucket = self.capacity / INNER_BUCKETS; // e.g. 1M/1024 = 976
        let mut out = [0usize; INNER_PROJECTIONS];
        for proj in 0..INNER_PROJECTIONS {
            let bucket = inner_bucket(&vector, proj); // [0, 1024)
            out[proj] = bucket * slots_per_bucket;    // start of that bucket's region
        }
        out
    }

    pub fn insert(&mut self, vector: U1024, payload_id: u64) -> Result<(), &'static str> {
        let starts = self.lsh_indices(vector);
        let max_size = self.capacity;
        let depth = self.probe_depth;
        let arena = self.arena_mut();

        for &start in &starts {
            for probe in 0..depth {
                let idx = (start + probe) % max_size;
                if arena[idx].is_active == 0 {
                    arena[idx].vector = vector;
                    arena[idx].payload_id = payload_id;
                    compiler_fence(Ordering::SeqCst);
                    arena[idx].is_active = 1;
                    return Ok(());
                }
            }
        }
        Err("LSH neighborhood capacity reached.")
    }

    pub fn search(&self, query: U1024, k: usize) -> Vec<(u64, u32)> {
        let starts = self.lsh_indices(query);
        let max_size = self.capacity;
        let depth = self.probe_depth;
        let arena = self.arena();

        let mut candidates: Vec<(u64, u32)> = Vec::with_capacity(depth * INNER_PROJECTIONS);

        for &start in &starts {
            for probe in 0..depth {
                let idx = (start + probe) % max_size;
                let record = &arena[idx];
                if record.is_active == 1 {
                    let distance = (record.vector ^ query).count_ones();
                    candidates.push((record.payload_id, distance));
                }
            }
        }

        candidates.sort_unstable_by_key(|&(_, dist)| dist);
        candidates.dedup_by_key(|(id, _)| *id);
        candidates.truncate(k);
        candidates
    }

    pub fn get(&self, payload_id: u64) -> Option<U1024> {
        let arena = self.arena();
        for i in 0..self.capacity {
            if arena[i].is_active == 1 && arena[i].payload_id == payload_id {
                return Some(arena[i].vector);
            }
        }
        None
    }

    pub fn delete(&mut self, payload_id: u64) -> bool {
        let cap = self.capacity;
        let arena = self.arena_mut();
        for i in 0..cap {
            if arena[i].is_active == 1 && arena[i].payload_id == payload_id {
                arena[i].is_active = 0;
                return true;
            }
        }
        false
    }

    pub fn capacity(&self) -> usize { self.capacity }

    pub fn len(&self) -> usize {
        let arena = self.arena();
        (0..self.capacity).filter(|&i| arena[i].is_active == 1).count()
    }
}