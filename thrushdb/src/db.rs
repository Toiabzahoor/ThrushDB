// src/db.rs

use memmap2::MmapMut;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{compiler_fence, Ordering};
use std::sync::RwLock;

use crate::u1024::U1024;

pub const DEFAULT_CLUSTER_DIR: &str = "data/thrush_cluster";

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VectorRecord {
    pub vector: U1024,      // 128 bytes: The binary embedding itself
    pub payload_id: u64,    // 8 bytes: A unique ID tying this vector to the document
    pub is_active: u8,      // 1 byte: 1 if occupied, 0 if empty
    pub _pad: [u8; 7],      // 7 bytes: Padding to perfectly align the struct to 144 bytes
}

impl Default for VectorRecord {
    fn default() -> Self {
        Self {
            vector: U1024::ZERO,
            payload_id: 0,
            is_active: 0,
            _pad: [0; 7],
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ThrushCluster (The Semantic Router)
// ─────────────────────────────────────────────────────────────────────────────

pub struct ThrushCluster {
    chunks: Vec<RwLock<ThrushDB>>, 
    num_chunks: usize,
}

impl ThrushCluster {
    /// Initializes a cluster. 
    /// `probe_depth` controls the LSH slot limit (e.g., 64 for speed, 256 for high recall).
    pub fn new<P: AsRef<Path>>(
        dir: P, 
        num_chunks: usize, 
        chunk_capacity: usize, 
        probe_depth: usize
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
    fn router_hash(&self, vector: U1024) -> usize {
        // TRUE LSH: We use the raw value of the first 64 bits.
        // Vectors with identical prefixes are mathematically forced into the same chunk.
        (vector.0[0] as usize) % self.num_chunks
    }

    pub fn insert(&self, vector: U1024, payload_id: u64) -> Result<(), &'static str> {
        let base_chunk_id = self.router_hash(vector);

        for offset in 0..self.num_chunks {
            let chunk_id = (base_chunk_id + offset) % self.num_chunks;
            let mut chunk_guard = self.chunks[chunk_id].write().unwrap();
            
            match chunk_guard.insert(vector, payload_id) {
                Ok(_) => return Ok(()),
                Err(_) => continue, 
            }
        }

        Err("CRITICAL: Cluster is entirely full. All neighborhoods and adjacent chunks maxed out.")
    }

    pub fn search(&self, query: U1024, k: usize) -> Vec<(u64, u32)> {
        let base_chunk_id = self.router_hash(query);
        let mut all_candidates = Vec::with_capacity(k * 2);

        for offset in 0..self.num_chunks {
            let chunk_id = (base_chunk_id + offset) % self.num_chunks;
            
            let chunk_guard = self.chunks[chunk_id].read().unwrap();
            let (mut candidates, spilled) = chunk_guard.search(query, k);
            all_candidates.append(&mut candidates);

            // If we didn't spill over the memory boundary, we can stop searching.
            if !spilled || all_candidates.len() >= k * 2 {
                break;
            }
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
        for chunk_lock in &self.chunks {
            let mut chunk = chunk_lock.write().unwrap();
            if chunk.delete(payload_id) {
                return true;
            }
        }
        false
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
// ThrushDB (The Core Engine)
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

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let needed_size = (capacity * std::mem::size_of::<VectorRecord>()) as u64;
        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        
        if current_size != needed_size {
            file.set_len(needed_size)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let mmap_ptr = mmap.as_ptr();
        let mut _dummy = 0;
        for i in (0..mmap.len()).step_by(4096) {
            unsafe {
                _dummy ^= std::ptr::read_volatile(mmap_ptr.add(i));
            }
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

    const LSH_PROJECTIONS: usize = 8;

    #[inline(always)]
    pub fn lsh_indices(&self, vector: U1024) -> [usize; Self::LSH_PROJECTIONS] {
        let mut out = [0usize; Self::LSH_PROJECTIONS];
        // TRUE LSH: We use the subsequent 64-bit blocks directly as memory addresses.
        // We removed the bit-scrambling. 
        for proj in 0..Self::LSH_PROJECTIONS {
            out[proj] = (vector.0[proj + 1] as usize) % self.capacity;
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

    pub fn search(&self, query: U1024, k: usize) -> (Vec<(u64, u32)>, bool) {
        let starts = self.lsh_indices(query);
        let max_size = self.capacity;
        let depth = self.probe_depth;
        let arena = self.arena();
        
        let mut candidates: Vec<(u64, u32)> = Vec::with_capacity(512);
        let mut spilled = false;

        for &start in &starts {
            for probe in 0..depth {
                let idx = (start + probe) % max_size;
                let record = &arena[idx];

                if record.is_active == 1 {
                    let distance = (record.vector ^ query).count_ones();
                    candidates.push((record.payload_id, distance));
                    
                    if probe == depth - 1 { spilled = true; }
                }
            }
        }

        candidates.sort_unstable_by_key(|&(_, dist)| dist);
        candidates.dedup_by_key(|(id, _)| *id);
        candidates.truncate(k);
        (candidates, spilled)
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
        let mut count = 0;
        for i in 0..self.capacity {
            if arena[i].is_active == 1 { count += 1; }
        }
        count
    }
}