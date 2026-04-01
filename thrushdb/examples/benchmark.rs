// examples/benchmark.rs

use rand::Rng;
use std::fs;
use std::time::Instant;
use thrushdb::{ThrushDB, U1024};

fn generate_random_u1024<R: Rng>(rng: &mut R) -> U1024 {
    let mut data = [0u64; 16];
    for w in &mut data {
        *w = rng.r#gen();
    }
    U1024(data)
}

fn main() {
    let path = "data/benchmark_thrushdb.bin";
    let _ = fs::remove_file(path); // Start fresh

    let arena_size = 1_000_000;
    let num_searches = 1_000;
    
    println!("🚀 Starting ThrushDB Benchmark...");
    println!("Initializing memory-mapped arena (Capacity: 1,000,000 vectors)...");
    
    let mut db = ThrushDB::new_at(path, arena_size).unwrap();
    let mut rng = rand::thread_rng();

    // --- PHASE 1: INSERTION ---
    println!("Inserting 1,000,000 random vectors...");
    let start_insert = Instant::now();
    
    let mut successful_inserts = 0;
    for i in 0..arena_size {
        let vec = generate_random_u1024(&mut rng);
        if db.insert(vec, i as u64).is_ok() {
            successful_inserts += 1;
        }
    }
    
    let insert_duration = start_insert.elapsed();
    println!("✅ Inserted {} vectors in {:.2?}", successful_inserts, insert_duration);

    // --- PHASE 2: SEARCHING ---
    println!("\nGenerating {} random queries...", num_searches);
    let mut queries = Vec::with_capacity(num_searches);
    for _ in 0..num_searches {
        queries.push(generate_random_u1024(&mut rng));
    }

    println!("Executing {} Top-5 searches...", num_searches);
    let start_search = Instant::now();
    
    for query in &queries {
        let _results = db.search(*query, 5);
    }
    
    let search_duration = start_search.elapsed();
    let avg_search_us = search_duration.as_micros() as f64 / num_searches as f64;

    println!("✅ Completed {} searches in {:.2?}", num_searches, search_duration);
    println!("\n🔥 PERFORMANCE METRICS 🔥");
    println!("--------------------------------------");
    println!("Average Latency per Search: {:.2} µs (microseconds)", avg_search_us);
    println!("Searches per Second (QPS):  {:.0}", 1_000_000.0 / avg_search_us);
    println!("--------------------------------------");

    // Cleanup
    let _ = fs::remove_file(path);
}