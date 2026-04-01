// src/db.rs

use memmap2::MmapMut;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{compiler_fence, Ordering};

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
// ThrushCluster (The Semantic Router / LSH Orchestrator)
// ─────────────────────────────────────────────────────────────────────────────

pub struct ThrushCluster {
    chunks: Vec<ThrushDB>,
    num_chunks: usize,
}

impl ThrushCluster {
    /// Initializes a cluster of ThrushDB instances, routing vectors via LSH.
    /// `chunk_capacity` should ideally map to your CPU cache targets (e.g., 1_000_000).
    pub fn new<P: AsRef<Path>>(dir: P, num_chunks: usize, chunk_capacity: usize) -> io::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let mut chunks = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let path = dir.join(format!("chunk_{}.bin", i));
            chunks.push(ThrushDB::new_at(path, chunk_capacity)?);
        }

        Ok(Self { chunks, num_chunks })
    }

    /// Uses the first LSH projection as a deterministic router to pick the ideal chunk.
    #[inline(always)]
    fn router_hash(&self, vector: U1024) -> usize {
        // Explicitly typed as u64 to satisfy the Rust compiler's strict type inference
        let seed: u64 = 0x9e3779b97f4a7c15;
        let mut h = seed;
        for w in 0..16usize {
            h ^= vector.0[w]
                .wrapping_mul(seed.wrapping_add((w as u64).wrapping_mul(0x517cc1b727220a95)));
            h = h.rotate_left(((w * 3) % 63 + 1) as u32);
            h ^= h >> 33;
            h = h.wrapping_mul(0xff51afd7ed558ccd);
            h ^= h >> 33;
        }
        (h as usize) % self.num_chunks
    }

    /// Inserts a vector. If a chunk's LSH neighborhood is full, it seamlessly spills to the next.
    pub fn insert(&mut self, vector: U1024, payload_id: u64) -> Result<(), &'static str> {
        let base_chunk_id = self.router_hash(vector);

        for offset in 0..self.num_chunks {
            let chunk_id = (base_chunk_id + offset) % self.num_chunks;
            
            // Attempt to insert into the target chunk
            match self.chunks[chunk_id].insert(vector, payload_id) {
                Ok(_) => return Ok(()),
                Err(_) => {
                    // Neighborhood is maxed out. Let it loop and spill to chunk_id + 1
                    continue; 
                }
            }
        }

        Err("CRITICAL: Cluster is entirely full. All neighborhoods and adjacent chunks maxed out.")
    }

    /// Searches the routed chunk. If the chunk hit maximum density, it peeks into the adjacent chunk.
    pub fn search(&self, query: U1024, k: usize) -> Vec<(u64, u32)> {
        let base_chunk_id = self.router_hash(query);
        let mut all_candidates = Vec::with_capacity(k * 2);

        for offset in 0..self.num_chunks {
            let chunk_id = (base_chunk_id + offset) % self.num_chunks;
            
            // search() now returns a boolean flagging if we hit the edge of a 64-slot neighborhood
            let (mut candidates, spilled) = self.chunks[chunk_id].search(query, k);
            all_candidates.append(&mut candidates);

            // If the neighborhood wasn't completely maxed out, we know it didn't spill during insert.
            // We can safely ignore all other chunks and break the loop early.
            if !spilled || all_candidates.len() >= k * 2 {
                break;
            }
        }

        all_candidates.sort_unstable_by_key(|&(_, dist)| dist);
        all_candidates.dedup_by_key(|(id, _)| *id);
        all_candidates.into_iter().take(k).collect()
    }

    pub fn get(&self, payload_id: u64) -> Option<U1024> {
        // ID lookup doesn't have the vector to route, so we scan chunks (still extremely fast)
        for chunk in &self.chunks {
            if let Some(vec) = chunk.get(payload_id) {
                return Some(vec);
            }
        }
        None
    }

    pub fn delete(&mut self, payload_id: u64) -> bool {
        for chunk in &mut self.chunks {
            if chunk.delete(payload_id) {
                return true;
            }
        }
        false
    }

    pub fn flush(&mut self) -> io::Result<()> {
        for chunk in &mut self.chunks {
            chunk.flush()?;
        }
        Ok(())
    }

    pub fn total_capacity(&self) -> usize {
        self.chunks.iter().map(|c| c.capacity()).sum()
    }

    pub fn total_len(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ThrushDB (The 1-Million Record Cache-Friendly Engine)
// ─────────────────────────────────────────────────────────────────────────────

pub struct ThrushDB {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
}

unsafe impl Send for ThrushDB {}
unsafe impl Sync for ThrushDB {}

impl ThrushDB {
    pub fn new_at<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
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

        // Cache Warmup
        let mmap_ptr = mmap.as_ptr();
        let mut _dummy = 0;
        for i in (0..mmap.len()).step_by(4096) {
            unsafe {
                _dummy ^= std::ptr::read_volatile(mmap_ptr.add(i));
            }
        }

        Ok(Self {
            _file: file,
            mmap,
            capacity,
        })
    }

    #[inline]
    pub(crate) fn arena(&self) -> &[VectorRecord] {
        unsafe {
            std::slice::from_raw_parts(
                self.mmap.as_ptr() as *const VectorRecord,
                self.capacity,
            )
        }
    }

    #[inline]
    pub(crate) fn arena_mut(&mut self) -> &mut [VectorRecord] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.mmap.as_mut_ptr() as *mut VectorRecord,
                self.capacity,
            )
        }
    }

    pub fn flush(&mut self) -> io::Result<()> { 
        self.mmap.flush() 
    }

    const LSH_PROJECTIONS: usize = 8;
    const LSH_SEEDS: [u64; 8] = [
        0x9e3779b97f4a7c15, 0x6c62272e07bb0142,
        0xd2a98b26625eee7b, 0x94d049bb133111eb,
        0xbf58476d1ce4e5b9, 0x517cc1b727220a95,
        0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
    ];

    #[inline(always)]
    pub fn lsh_indices(&self, vector: U1024) -> [usize; Self::LSH_PROJECTIONS] {
        let mut out = [0usize; Self::LSH_PROJECTIONS];
        for (proj, &seed) in Self::LSH_SEEDS.iter().enumerate() {
            let mut h = seed;
            for w in 0..16usize {
                h ^= vector.0[w]
                    .wrapping_mul(seed.wrapping_add((w as u64).wrapping_mul(0x517cc1b727220a95)));
                h = h.rotate_left(((proj * 5 + w * 3) % 63 + 1) as u32);
                h ^= h >> 33;
                h = h.wrapping_mul(0xff51afd7ed558ccd);
                h ^= h >> 33;
            }
            out[proj] = (h as usize) % self.capacity;
        }
        out
    }

    pub fn insert(&mut self, vector: U1024, payload_id: u64) -> Result<(), &'static str> {
        let starts = self.lsh_indices(vector);
        let max_size = self.capacity; 
        let arena = self.arena_mut();

        for &start in &starts {
            for probe in 0..64usize {
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
        let arena = self.arena();
        
        let mut candidates: Vec<(u64, u32)> = Vec::with_capacity(512);
        let mut spilled = false;

        for &start in &starts {
            for probe in 0..64usize {
                let idx = (start + probe) % max_size;
                let record = &arena[idx];

                if record.is_active == 1 {
                    let distance = (record.vector ^ query).count_ones();
                    candidates.push((record.payload_id, distance));
                    
                    if probe == 63 {
                        spilled = true;
                    }
                }
            }
        }

        candidates.sort_unstable_by_key(|&(_, dist)| dist);
        candidates.dedup_by_key(|(id, _)| *id);
        candidates.truncate(k);
        
        (candidates, spilled)
    }

    pub fn get(&self, payload_id: u64) -> Option<U1024> {
        let max_size = self.capacity;
        let arena = self.arena();

        for i in 0..max_size {
            if arena[i].is_active == 1 && arena[i].payload_id == payload_id {
                return Some(arena[i].vector);
            }
        }
        None
    }

    pub fn delete(&mut self, payload_id: u64) -> bool {
        let max_size = self.capacity;
        let arena = self.arena_mut();

        for i in 0..max_size {
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

    pub fn load_factor(&self) -> f64 {
        self.len() as f64 / self.capacity as f64
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_thrush_cluster_routing_and_crud() {
        let dir = "test_cluster_data";
        let _ = fs::remove_dir_all(dir);

        let mut cluster = ThrushCluster::new(dir, 3, 1000).unwrap();
        
        assert_eq!(cluster.total_capacity(), 3000);
        assert_eq!(cluster.total_len(), 0);

        let mut target_vec = U1024::ZERO;
        target_vec.0[0] = 0b1010;

        assert!(cluster.insert(target_vec, 99).is_ok());
        assert_eq!(cluster.total_len(), 1);

        let retrieved = cluster.get(99);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().0[0], 0b1010);

        let search_results = cluster.search(target_vec, 5);
        assert_eq!(search_results.len(), 1);
        assert_eq!(search_results[0].0, 99);

        let deleted = cluster.delete(99);
        assert!(deleted);
        assert_eq!(cluster.total_len(), 0);
        assert!(cluster.get(99).is_none());

        let _ = fs::remove_dir_all(dir);
    }
}