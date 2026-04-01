use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::time::Instant;

const FILE_SIZE_MB: usize = 512; // adjust if needed
const BLOCK_SIZE: usize = 4 * 1024; // 4 KB
const FILE_NAME: &str = "bench.dat";

fn main() {
    println!("Starting I/O benchmark...");

    let total_bytes = FILE_SIZE_MB * 1024 * 1024;
    let buffer = vec![0u8; BLOCK_SIZE];

    // ------------------------
    // Sequential Write
    // ------------------------
    let mut file = File::create(FILE_NAME).expect("create failed");
    let start = Instant::now();

    for _ in 0..(total_bytes / BLOCK_SIZE) {
        file.write_all(&buffer).unwrap();
    }
    file.sync_all().unwrap();

    let elapsed = start.elapsed().as_secs_f64();
    let mbps = FILE_SIZE_MB as f64 / elapsed;

    println!("Sequential Write: {:.2} MB/s", mbps);

    // ------------------------
    // Sequential Read
    // ------------------------
    let mut file = File::open(FILE_NAME).unwrap();
    let mut read_buf = vec![0u8; BLOCK_SIZE];

    let start = Instant::now();

    for _ in 0..(total_bytes / BLOCK_SIZE) {
        file.read_exact(&mut read_buf).unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mbps = FILE_SIZE_MB as f64 / elapsed;

    println!("Sequential Read: {:.2} MB/s", mbps);

    // ------------------------
    // Random Read
    // ------------------------
    let mut file = OpenOptions::new().read(true).open(FILE_NAME).unwrap();

    let iterations = total_bytes / BLOCK_SIZE;
    let start = Instant::now();

    for i in 0..iterations {
        let offset = ((i * 7919) % iterations) * BLOCK_SIZE; // pseudo-random
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.read_exact(&mut read_buf).unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let iops = iterations as f64 / elapsed;

    println!("Random Read: {:.0} IOPS", iops);

    // cleanup
    std::fs::remove_file(FILE_NAME).ok();
}