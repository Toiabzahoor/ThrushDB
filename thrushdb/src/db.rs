// src/db.rs

use memmap2::MmapMut;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{compiler_fence, Ordering};
use std::sync::{Mutex, RwLock};

use crate::u1024::U1024;

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────

pub struct ClusterConfig {
    pub initial_chunks: usize,
    pub max_chunks: usize, // 0 = Unlimited dynamic allocation
    pub chunk_capacity: usize,
    pub overflow_capacity: usize,
    pub probe_depth: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            initial_chunks: 16,
            max_chunks: 0, 
            chunk_capacity: 1_048_576, 
            overflow_capacity: 1_048_576, 
            probe_depth: 256,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// METADATA STORE (Log-Structured Append-Only Storage)
// ─────────────────────────────────────────────────────────────────────────────

pub struct MetadataStore {
    file: Mutex<File>,
    index: RwLock<HashMap<u64, u64>>, // payload_id -> offset in file
}

impl MetadataStore {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;
        let mut index = HashMap::new();

        // Rebuild RAM index from the log file
        let mut offset = 0;
        let mut op_buf = [0u8; 1];
        
        file.seek(SeekFrom::Start(0))?;
        loop {
            if file.read_exact(&mut op_buf).is_err() { break; }
            offset += 1;
            let op = op_buf[0];

            if op == 1 { // OP_PUT
                let mut head = [0u8; 12]; // 8 bytes ID + 4 bytes Len
                if file.read_exact(&mut head).is_err() { break; }
                let id = u64::from_le_bytes(head[0..8].try_into().unwrap());
                let len = u32::from_le_bytes(head[8..12].try_into().unwrap());
                
                index.insert(id, offset); 
                
                file.seek(SeekFrom::Current(len as i64))?;
                offset += 12 + len as u64;
            } else if op == 2 { // OP_DELETE
                let mut id_buf = [0u8; 8];
                if file.read_exact(&mut id_buf).is_err() { break; }
                let id = u64::from_le_bytes(id_buf);
                index.remove(&id);
                offset += 8;
            }
        }

        Ok(Self {
            file: Mutex::new(file),
            index: RwLock::new(index),
        })
    }

    pub fn put(&self, id: u64, data: &str) -> io::Result<()> {
        let mut file = self.file.lock().unwrap();
        let offset = file.seek(SeekFrom::End(0))?;
        
        let bytes = data.as_bytes();
        let len = bytes.len() as u32;
        
        let mut record = Vec::with_capacity(1 + 8 + 4 + bytes.len());
        record.push(1); // OP_PUT
        record.extend_from_slice(&id.to_le_bytes());
        record.extend_from_slice(&len.to_le_bytes());
        record.extend_from_slice(bytes);
        
        file.write_all(&record)?;
        self.index.write().unwrap().insert(id, offset + 1); // +1 skips the OpCode
        Ok(())
    }

    pub fn get(&self, id: u64) -> Option<String> {
        let offset = {
            let idx = self.index.read().unwrap();
            *idx.get(&id)?
        };

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset)).ok()?;
        
        let mut head = [0u8; 12];
        file.read_exact(&mut head).ok()?;
        let len = u32::from_le_bytes(head[8..12].try_into().unwrap());
        
        let mut data = vec![0u8; len as usize];
        file.read_exact(&mut data).ok()?;
        
        String::from_utf8(data).ok()
    }

    pub fn delete(&self, id: u64) -> io::Result<()> {
        let mut idx = self.index.write().unwrap();
        if idx.remove(&id).is_some() {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::End(0))?;
            let mut record = vec![2]; // OP_DELETE
            record.extend_from_slice(&id.to_le_bytes());
            file.write_all(&record)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> io::Result<()> {
        self.file.lock().unwrap().sync_all()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TWO-LEVEL LSH DESIGN
// ─────────────────────────────────────────────────────────────────────────────

const NUM_TABLES: usize = 3;
const CLUSTER_HASH_BITS: [[(usize, u64); 8]; NUM_TABLES] = [
    [(0,5),  (2,11), (4,17), (6,23), (8,29),  (10,37), (12,41), (14,47)],
    [(1,3),  (3,7),  (5,13), (7,19), (9,31),  (11,43), (13,53), (15,59)],
    [(0,41), (2,47), (4,53), (6,59), (8,7),   (10,13), (12,19), (14,29)],
];

#[inline(always)]
fn cluster_table_hash(vector: &U1024, table: usize) -> usize {
    let b = &CLUSTER_HASH_BITS[table];
    let mut h = 0;
    for k in 0..8 { h |= (((vector.0[b[k].0] >> b[k].1) & 1) as usize) << k; }
    h 
}

const INNER_PROJECTIONS: usize = 8;
const INNER_K: usize = 10;
const INNER_BUCKETS: usize = 1 << INNER_K;

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
    let mut h = 0;
    for k in 0..INNER_K { h |= (((vector.0[b[k].0] >> b[k].1) & 1) as usize) << k; }
    h
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VectorRecord {
    pub vector: U1024,
    pub payload_id: u64,
    pub is_active: u8,
    pub _pad: [u8; 7],
}

impl Default for VectorRecord {
    fn default() -> Self { Self { vector: U1024::ZERO, payload_id: 0, is_active: 0, _pad: [0; 7] } }
}

// ─────────────────────────────────────────────────────────────────────────────
// ThrushCluster (Router, WAL & Metadata Manager)
// ─────────────────────────────────────────────────────────────────────────────

pub struct ThrushCluster {
    chunks: RwLock<Vec<RwLock<ThrushDB>>>,
    routing_tables: [RwLock<[usize; 256]>; NUM_TABLES],
    base_dir: PathBuf,
    pub config: ClusterConfig,
    
    // Durable Storage Additions
    wal_file: Mutex<File>,
    pub metadata: MetadataStore,
}

impl ThrushCluster {
    pub fn new<P: AsRef<Path>>(dir: P, config: ClusterConfig) -> io::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let mut initial = config.initial_chunks;
        if config.max_chunks > 0 && initial > config.max_chunks { initial = config.max_chunks; }

        let mut chunks = Vec::with_capacity(initial);
        for i in 0..initial {
            let path = dir.join(format!("chunk_{}.bin", i));
            let db = ThrushDB::new_at(&path, config.chunk_capacity, config.overflow_capacity, config.probe_depth)?;
            chunks.push(RwLock::new(db));
        }

        let tables = [RwLock::new([0; 256]), RwLock::new([0; 256]), RwLock::new([0; 256])];
        for t in 0..NUM_TABLES {
            let mut table_guard = tables[t].write().unwrap();
            for slot in 0..256 { table_guard[slot] = slot % initial.max(1); }
        }

        // Initialize MetaStore and WAL
        let metadata = MetadataStore::new(dir.join("metadata.log"))?;
        let wal_path = dir.join("wal.bin");
        let mut wal_file = OpenOptions::new().read(true).write(true).create(true).open(&wal_path)?;

        let cluster = Self {
            chunks: RwLock::new(chunks),
            routing_tables: tables,
            base_dir: dir.to_path_buf(),
            config,
            wal_file: Mutex::new(wal_file.try_clone()?),
            metadata,
        };

        // Replay WAL on Boot
        cluster.replay_wal(&mut wal_file)?;

        Ok(cluster)
    }

    fn replay_wal(&self, wal_file: &mut File) -> io::Result<()> {
        wal_file.seek(SeekFrom::Start(0))?;
        let mut op = [0u8; 1];
        
        while wal_file.read_exact(&mut op).is_ok() {
            if op[0] == 1 { // OP_INSERT
                let mut id_buf = [0u8; 8];
                wal_file.read_exact(&mut id_buf)?;
                let id = u64::from_le_bytes(id_buf);
                
                let mut vec_buf = [0u8; 128];
                wal_file.read_exact(&mut vec_buf)?;
                let mut u1024_data = [0u64; 16];
                for i in 0..16 {
                    u1024_data[i] = u64::from_le_bytes(vec_buf[i*8..(i+1)*8].try_into().unwrap());
                }
                
                let _ = self.insert_internal(U1024(u1024_data), id);
            } else if op[0] == 2 { // OP_DELETE
                let mut id_buf = [0u8; 8];
                wal_file.read_exact(&mut id_buf)?;
                let id = u64::from_le_bytes(id_buf);
                self.delete_internal(id);
            }
        }
        
        // Truncate WAL after successful replay
        wal_file.set_len(0)?;
        wal_file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    #[inline(always)]
    fn target_chunks(&self, vector: U1024) -> [usize; NUM_TABLES] {
        let h0 = cluster_table_hash(&vector, 0);
        let h1 = cluster_table_hash(&vector, 1);
        let h2 = cluster_table_hash(&vector, 2);
        [
            self.routing_tables[0].read().unwrap()[h0],
            self.routing_tables[1].read().unwrap()[h1],
            self.routing_tables[2].read().unwrap()[h2],
        ]
    }

    /// Primary insert method capable of storing vectors and optional text metadata
    pub fn insert(&self, vector: U1024, payload_id: u64, text_metadata: Option<&str>) -> Result<(), &'static str> {
        self.insert_internal(vector, payload_id)?;
        
        if let Some(meta) = text_metadata {
            let _ = self.metadata.put(payload_id, meta);
        }
        
        // Write to durable WAL
        let mut file = self.wal_file.lock().unwrap();
        let mut buf = Vec::with_capacity(1 + 8 + 128);
        buf.push(1); // OP_INSERT
        buf.extend_from_slice(&payload_id.to_le_bytes());
        for w in &vector.0 { buf.extend_from_slice(&w.to_le_bytes()); }
        let _ = file.write_all(&buf);

        Ok(())
    }

    fn insert_internal(&self, vector: U1024, payload_id: u64) -> Result<(), &'static str> {
        let targets = self.target_chunks(vector);
        let mut any_ok = false;
        let cluster_guard = self.chunks.read().unwrap();
        
        for chunk_id in targets {
            let mut chunk_guard = cluster_guard[chunk_id].write().unwrap();
            if chunk_guard.insert(vector, payload_id).is_ok() { any_ok = true; }
        }
        if any_ok { Ok(()) } else { Err("Insertion failed across all targets.") }
    }

    /// Search now returns (PayloadID, Distance, Optional Metadata String)
    pub fn search(&self, query: U1024, k: usize) -> Vec<(u64, u32, Option<String>)> {
        let targets = self.target_chunks(query);
        let mut all_candidates = Vec::with_capacity(k * NUM_TABLES);
        
        let cluster_guard = self.chunks.read().unwrap();
        for chunk_id in targets {
            let chunk_guard = cluster_guard[chunk_id].read().unwrap();
            all_candidates.append(&mut chunk_guard.search(query, k));
        }
        
        all_candidates.sort_unstable_by_key(|&(_, dist)| dist);
        all_candidates.dedup_by_key(|(id, _)| *id);
        
        all_candidates.into_iter().take(k).map(|(id, dist)| {
            (id, dist, self.metadata.get(id))
        }).collect()
    }

    pub fn delete(&self, payload_id: u64) -> bool {
        if self.delete_internal(payload_id) {
            let _ = self.metadata.delete(payload_id);
            
            // Write to durable WAL
            let mut file = self.wal_file.lock().unwrap();
            let mut buf = [0u8; 9];
            buf[0] = 2; // OP_DELETE
            buf[1..9].copy_from_slice(&payload_id.to_le_bytes());
            let _ = file.write_all(&buf);
            
            return true;
        }
        false
    }

    fn delete_internal(&self, payload_id: u64) -> bool {
        let mut deleted = false;
        let cluster_guard = self.chunks.read().unwrap();
        for chunk_lock in cluster_guard.iter() {
            if chunk_lock.write().unwrap().delete(payload_id) { deleted = true; }
        }
        deleted
    }

    pub fn flush(&self) -> io::Result<()> {
        let cluster_guard = self.chunks.read().unwrap();
        for chunk_lock in cluster_guard.iter() { chunk_lock.write().unwrap().flush()?; }
        
        self.metadata.flush()?;

        // Mmaps are safe on disk. Truncate the WAL to save space.
        let mut wal = self.wal_file.lock().unwrap();
        wal.set_len(0)?;
        wal.seek(SeekFrom::Start(0))?;

        Ok(())
    }

    pub fn try_expand(&self, table_idx: usize, slot_idx: usize) -> Result<(), &'static str> {
        let mut cluster_guard = self.chunks.write().unwrap();
        let current_count = cluster_guard.len();
        if self.config.max_chunks != 0 && current_count >= self.config.max_chunks { return Err("Max chunks reached."); }

        let path = self.base_dir.join(format!("chunk_{}.bin", current_count));
        let db = ThrushDB::new_at(&path, self.config.chunk_capacity, self.config.overflow_capacity, self.config.probe_depth)
            .map_err(|_| "Disk allocation failed")?;
            
        cluster_guard.push(RwLock::new(db));
        self.routing_tables[table_idx].write().unwrap()[slot_idx] = current_count;
        Ok(())
    }

    pub fn total_capacity(&self) -> usize { self.chunks.read().unwrap().iter().map(|c| c.read().unwrap().capacity()).sum() }
    pub fn total_len(&self) -> usize { self.chunks.read().unwrap().iter().map(|c| c.read().unwrap().len()).sum() }
    pub fn get(&self, payload_id: u64) -> Option<(U1024, Option<String>)> {
        let cluster_guard = self.chunks.read().unwrap();
        for chunk_lock in cluster_guard.iter() {
            if let Some(vec) = chunk_lock.read().unwrap().get(payload_id) { 
                return Some((vec, self.metadata.get(payload_id))); 
            }
        }
        None
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ThrushDB (Core Engine with Vertical Overflows)
// ─────────────────────────────────────────────────────────────────────────────

pub struct ThrushDB {
    pub path: PathBuf,
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    overflow_capacity: usize,
    probe_depth: usize,
    pub overflows: Vec<ThrushDB>, 
}

unsafe impl Send for ThrushDB {}
unsafe impl Sync for ThrushDB {}

impl ThrushDB {
    pub fn new_at<P: AsRef<Path>>(path: P, capacity: usize, overflow_capacity: usize, probe_depth: usize) -> io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;

        let needed_size = (capacity * std::mem::size_of::<VectorRecord>()) as u64;
        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        if current_size != needed_size { file.set_len(needed_size)?; }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self { 
            path: path.to_path_buf(), _file: file, mmap, capacity, overflow_capacity, probe_depth, overflows: Vec::new() 
        })
    }

    #[inline] pub(crate) fn arena(&self) -> &[VectorRecord] { unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const VectorRecord, self.capacity) } }
    #[inline] pub(crate) fn arena_mut(&mut self) -> &mut [VectorRecord] { unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr() as *mut VectorRecord, self.capacity) } }

    pub fn flush(&mut self) -> io::Result<()> { 
        self.mmap.flush()?;
        for overflow in &mut self.overflows { overflow.flush()?; }
        Ok(())
    }

    #[inline(always)]
    pub fn lsh_indices(&self, vector: U1024) -> [usize; INNER_PROJECTIONS] {
        let slots_per_bucket = self.capacity / INNER_BUCKETS;
        let mut out = [0usize; INNER_PROJECTIONS];
        for proj in 0..INNER_PROJECTIONS { out[proj] = inner_bucket(&vector, proj) * slots_per_bucket; }
        out
    }

    pub fn insert(&mut self, vector: U1024, payload_id: u64) -> Result<(), &'static str> {
        if self.try_insert_local(vector, payload_id) { return Ok(()); }
        for overflow in &mut self.overflows { if overflow.try_insert_local(vector, payload_id) { return Ok(()); } }

        let overflow_id = self.overflows.len() + 1;
        let mut new_path = self.path.clone();
        let new_filename = format!("{}_overflow_{}.bin", self.path.file_stem().unwrap().to_str().unwrap(), overflow_id);
        new_path.set_file_name(new_filename);
        
        let mut new_overflow = ThrushDB::new_at(&new_path, self.overflow_capacity, self.overflow_capacity, self.probe_depth)
            .map_err(|_| "Failed to map new overflow chunk to disk")?;
        
        new_overflow.try_insert_local(vector, payload_id);
        self.overflows.push(new_overflow);
        Ok(())
    }

    fn try_insert_local(&mut self, vector: U1024, payload_id: u64) -> bool {
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
                    return true;
                }
            }
        }
        false
    }

    pub fn search(&self, query: U1024, _k: usize) -> Vec<(u64, u32)> {
        let mut candidates = self.search_local(query);
        for overflow in &self.overflows { candidates.append(&mut overflow.search_local(query)); }
        candidates
    }

    fn search_local(&self, query: U1024) -> Vec<(u64, u32)> {
        let starts = self.lsh_indices(query);
        let max_size = self.capacity;
        let depth = self.probe_depth;
        let arena = self.arena();
        let mut local_candidates = Vec::with_capacity(depth * INNER_PROJECTIONS);

        for &start in &starts {
            for probe in 0..depth {
                let idx = (start + probe) % max_size;
                let record = &arena[idx];
                if record.is_active == 1 {
                    local_candidates.push((record.payload_id, (record.vector ^ query).count_ones()));
                }
            }
        }
        local_candidates
    }

    pub fn get(&self, payload_id: u64) -> Option<U1024> {
        let arena = self.arena();
        for i in 0..self.capacity { if arena[i].is_active == 1 && arena[i].payload_id == payload_id { return Some(arena[i].vector); } }
        for overflow in &self.overflows { if let Some(vec) = overflow.get(payload_id) { return Some(vec); } }
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
        for overflow in &mut self.overflows { if overflow.delete(payload_id) { return true; } }
        false
    }

    pub fn capacity(&self) -> usize { self.capacity + self.overflows.iter().map(|o| o.capacity()).sum::<usize>() }
    pub fn len(&self) -> usize {
        let arena = self.arena();
        let local_len = (0..self.capacity).filter(|&i| arena[i].is_active == 1).count();
        local_len + self.overflows.iter().map(|o| o.len()).sum::<usize>()
    }
}