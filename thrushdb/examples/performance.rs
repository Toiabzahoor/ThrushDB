// examples/real_world_sim.rs

use rand::Rng;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use thrushdb::{ClusterConfig, ThrushCluster, U1024};

fn generate_random_vector<R: Rng>(rng: &mut R) -> U1024 {
    let mut data = [0u64; 16];
    for w in &mut data {
        *w = rng.r#gen();
    }
    U1024(data)
}

fn main() {
    let dir = "data/basic_benchmark";
    let _ = fs::remove_dir_all(dir);

    let config = ClusterConfig {
        initial_chunks: 15,
        max_chunks: 0,
        chunk_capacity: 1_000_000,
        overflow_capacity: 1_000_000,
        probe_depth: 64,
    };
    
    let cluster = Arc::new(ThrushCluster::new(dir, config).unwrap());
    
    let num_threads = 4;
    let inserts_per_thread = 50_000;
    let total_inserts = num_threads * inserts_per_thread;
    
    println!("Starting insert phase...");
    println!("Inserting {} vectors...", total_inserts);
    
    let start_insert = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let c = Arc::clone(&cluster);
        handles.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for i in 0..inserts_per_thread {
                let vec = generate_random_vector(&mut rng);
                let id = (thread_id * inserts_per_thread + i) as u64;
                let _ = c.insert(vec, id, None);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let insert_duration = start_insert.elapsed();
    let ips = total_inserts as f64 / insert_duration.as_secs_f64();
    
    println!("Insert time: {:.2?}", insert_duration);
    println!("Insert throughput: {:.0} IPS\n", ips);

    let searches_per_thread = 5_000;
    let total_searches = num_threads * searches_per_thread;
    
    println!("Starting search phase...");
    println!("Searching {} vectors...", total_searches);
    
    let start_search = Instant::now();
    let mut search_handles = vec![];

    for _ in 0..num_threads {
        let c = Arc::clone(&cluster);
        search_handles.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for _ in 0..searches_per_thread {
                let vec = generate_random_vector(&mut rng);
                let _ = c.search(vec, 5);
            }
        }));
    }

    for handle in search_handles {
        handle.join().unwrap();
    }

    let search_duration = start_search.elapsed();
    let qps = total_searches as f64 / search_duration.as_secs_f64();
    let avg_latency = search_duration.as_micros() as f64 / total_searches as f64;
    
    println!("Search time: {:.2?}", search_duration);
    println!("Search throughput: {:.0} QPS", qps);
    println!("Average search latency: {:.2} µs", avg_latency);

    let _ = fs::remove_dir_all(dir);
}