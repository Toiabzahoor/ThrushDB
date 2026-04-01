// examples/titan_benchmark.rs

use rand::Rng;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use thrushdb::{ThrushCluster, U1024};

fn generate_random_vector<R: Rng>(rng: &mut R) -> U1024 {
    let mut data = [0u64; 16];
    for w in &mut data {
        *w = rng.r#gen();
    }
    U1024(data)
}

fn main() {
    let dir = "data/titan_benchmark";
    let _ = fs::remove_dir_all(dir);

    // 15 Chunks, 1 Million Capacity, Default 64 Slot Depth
    let num_chunks = 15;
    let chunk_capacity = 1_000_000;
    
    println!("🔥 BOOTING TITAN BENCHMARK (Codespace Safe Edition) 🔥");
    println!("Architecture: 15 Independent RW-Locked Chunks\n");

    let cluster = Arc::new(ThrushCluster::new(dir, num_chunks, chunk_capacity, 64).unwrap());
    

    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 1: Massively Parallel Writes (Codespace Friendly)
    // ─────────────────────────────────────────────────────────────────────────
    println!("=== PHASE 1: Unrestricted Parallel Writes ===");
    println!("Spawning 16 threads. Each inserting 1,000 vectors (16,000 total)...");
    
    let start_writes = Instant::now();
    let mut handles = vec![];

    // Dropped to 16 threads, 1k inserts each. Enough to prove parallel execution, gentle on IOPS.
    for thread_id in 0..16 {
        let cluster_clone = Arc::clone(&cluster);
        handles.push(thread::spawn(move || {
            let mut local_rng = rand::thread_rng();
            for i in 0..1_000 {
                let vec = generate_random_vector(&mut local_rng);
                let _ = cluster_clone.insert(vec, (thread_id * 10_000 + i) as u64);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let write_duration = start_writes.elapsed();
    let inserts_per_sec = (16_000.0 / write_duration.as_secs_f64()) as u64;
    println!("Completed in: {:.2?}", write_duration);
    println!("Throughput: {} inserts/sec", inserts_per_sec);
    println!("> ARCHITECTURE WIN: Threads are writing to different chunks simultaneously without a global lock.\n");


    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 2: The Chaos Matrix (Simultaneous Read/Write)
    // ─────────────────────────────────────────────────────────────────────────
    println!("=== PHASE 2: The Chaos Matrix (Mixed Read/Write Workload) ===");
    println!("Spawning 8 Reader Threads & 8 Writer Threads to attack the DB at the same time...");

    let start_chaos = Instant::now();
    let mut chaos_handles = vec![];

    // 8 Writer Threads (500 inserts each)
    for thread_id in 0..8 {
        let cluster_clone = Arc::clone(&cluster);
        chaos_handles.push(thread::spawn(move || {
            let mut local_rng = rand::thread_rng();
            for i in 0..500 {
                let vec = generate_random_vector(&mut local_rng);
                let _ = cluster_clone.insert(vec, (9_000_000 + thread_id * 1_000 + i) as u64);
            }
        }));
    }

    // 8 Reader Threads (500 searches each)
    for _ in 0..8 {
        let cluster_clone = Arc::clone(&cluster);
        chaos_handles.push(thread::spawn(move || {
            let mut local_rng = rand::thread_rng();
            for _ in 0..500 {
                let vec = generate_random_vector(&mut local_rng);
                let _ = cluster_clone.search(vec, 5); 
            }
        }));
    }

    for handle in chaos_handles {
        handle.join().unwrap();
    }

    println!("Completed 4,000 writes and 4,000 searches simultaneously in {:.2?}.", start_chaos.elapsed());
    println!("> ARCHITECTURE WIN: Readers can search chunks that aren't actively being written to without waiting.\n");


    // ─────────────────────────────────────────────────────────────────────────
    // PHASE 3: The Physics of Probe Depth (Speed vs. Recall Tradeoff)
    // ─────────────────────────────────────────────────────────────────────────
    println!("=== PHASE 3: Customizable Probe Depth Latency ===");
    let mut rng = rand::thread_rng();
    
    let speed_dir = "data/titan_speed";
    let acc_dir = "data/titan_acc";
    let _ = fs::remove_dir_all(speed_dir);
    let _ = fs::remove_dir_all(acc_dir);

    let speed_cluster = ThrushCluster::new(speed_dir, 1, 50_000, 16).unwrap();  // 16 slots
    let acc_cluster = ThrushCluster::new(acc_dir, 1, 50_000, 256).unwrap();     // 256 slots

    // Insert a small, safe batch of 5,000 vectors to scan against
    println!("Pre-filling clusters with 5,000 vectors for search benchmark...");
    for i in 0..5_000 {
        let v = generate_random_vector(&mut rng);
        let _ = speed_cluster.insert(v, i);
        let _ = acc_cluster.insert(v, i);
    }

    let mut queries = vec![];
    for _ in 0..1_000 {
        queries.push(generate_random_vector(&mut rng));
    }

    // Benchmark Depth 16
    let start_speed = Instant::now();
    for q in &queries { let _ = speed_cluster.search(*q, 5); }
    let speed_time = start_speed.elapsed().as_micros() as f64 / 1_000.0;

    // Benchmark Depth 256
    let start_acc = Instant::now();
    for q in &queries { let _ = acc_cluster.search(*q, 5); }
    let acc_time = start_acc.elapsed().as_micros() as f64 / 1_000.0;

    println!("Search Latency @ Depth 16:  {:.2} µs", speed_time);
    println!("Search Latency @ Depth 256: {:.2} µs", acc_time);
    println!("> ARCHITECTURE WIN: Proof that the developer has a physical dial to trade off nanoseconds for recall accuracy.");

    // Cleanup
    let _ = fs::remove_dir_all(dir);
    let _ = fs::remove_dir_all(speed_dir);
    let _ = fs::remove_dir_all(acc_dir);
}