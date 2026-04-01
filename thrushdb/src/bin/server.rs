// src/bin/server.rs

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{interval, Duration};

use thrushdb::{ClusterConfig, ThrushCluster, U1024};

// Protocol OpCodes (matching python/thrushdb.py)
const OP_INSERT: u8 = 0x01;
const OP_SEARCH: u8 = 0x02;
const OP_GET: u8 = 0x03;
const OP_DELETE: u8 = 0x04;
const OP_STATS: u8 = 0x05;

const STATUS_OK: u8 = 0x00;
const STATUS_ERR: u8 = 0x01;

/// Shared application state
struct AppState {
    cluster: Arc<ThrushCluster>,
}

#[tokio::main]
async fn main() {
    let dir = "data/thrush_live_tcp";
    
    let config = ClusterConfig {
        initial_chunks: 15,
        max_chunks: 0,
        chunk_capacity: 1_000_000,
        overflow_capacity: 1_000_000,
        probe_depth: 64,
    };

    println!("🔥 BOOTING THRUSH-DB TCP BINARY SERVER 🔥");
    println!("Initializing 15-Chunk Memory Mapped Cluster...");

    let cluster = Arc::new(ThrushCluster::new(dir, config).unwrap());
    let state = Arc::new(AppState {
        cluster: Arc::clone(&cluster),
    });

    println!("Cluster Ready. Capacity: {}", cluster.total_capacity());

    // Background Auto-Flusher
    let background_cluster = Arc::clone(&cluster);
    tokio::spawn(async move {
        let mut tick_interval = interval(Duration::from_secs(5));
        loop {
            tick_interval.tick().await;
            if let Err(e) = background_cluster.flush() {
                eprintln!("⚠️ Background flush warning: {}", e);
            }
        }
    });

    // Bind raw TCP Listener
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("🚀 TCP Binary Server listening on 0.0.0.0:3000");

    // Graceful shutdown hook
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("\n🛑 Received termination signal. Flushing chunks to disk...");
        let _ = cluster.flush();
        println!("✅ Flush complete. Exiting.");
        std::process::exit(0);
    });

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let state_clone = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, state_clone).await {
                        eprintln!("❌ Client error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
}

/// Handles the binary protocol for a single TCP connection
async fn handle_client(mut stream: TcpStream, state: Arc<AppState>) -> std::io::Result<()> {
    // Disable Nagle's algorithm for minimum latency
    let _ = stream.set_nodelay(true);

    loop {
        let mut opcode = [0u8; 1];
        // Read the 1-byte OpCode. If 0 bytes read, the client disconnected gracefully.
        if stream.read_exact(&mut opcode).await.is_err() {
            break; 
        }

        match opcode[0] {
            OP_INSERT => {
                let payload_id = stream.read_u64_le().await?;
                let mut vec_data = [0u64; 16];
                for i in 0..16 {
                    vec_data[i] = stream.read_u64_le().await?;
                }
                
                let meta_len = stream.read_u32_le().await?;
                let mut meta_bytes = vec![0u8; meta_len as usize];
                if meta_len > 0 {
                    stream.read_exact(&mut meta_bytes).await?;
                }

                // Parse metadata into String to pass as Option<&str>
                let meta_str = if meta_len > 0 {
                    String::from_utf8(meta_bytes).ok()
                } else {
                    None
                };

                match state.cluster.insert(U1024(vec_data), payload_id, meta_str.as_deref()) {
                    Ok(_) => stream.write_u8(STATUS_OK).await?,
                    Err(_) => stream.write_u8(STATUS_ERR).await?,
                }
            }

            OP_SEARCH => {
                let mut vec_data = [0u64; 16];
                for i in 0..16 {
                    vec_data[i] = stream.read_u64_le().await?;
                }
                let k = stream.read_u32_le().await? as usize;

                // Returns Vec<(u64, u32, Option<String>)> natively now
                let results = state.cluster.search(U1024(vec_data), k);

                stream.write_u8(STATUS_OK).await?;
                stream.write_u32_le(results.len() as u32).await?;

                for (id, dist, meta) in results {
                    stream.write_u64_le(id).await?;
                    stream.write_u32_le(dist).await?;
                    
                    if let Some(meta_text) = meta {
                        let bytes = meta_text.as_bytes();
                        stream.write_u32_le(bytes.len() as u32).await?;
                        stream.write_all(bytes).await?;
                    } else {
                        stream.write_u32_le(0).await?;
                    }
                }
            }

            OP_GET => {
                let payload_id = stream.read_u64_le().await?;
                
                // Returns Option<(U1024, Option<String>)> natively now
                if let Some((vec_obj, meta)) = state.cluster.get(payload_id) {
                    stream.write_u8(STATUS_OK).await?;
                    
                    for &val in &vec_obj.0 {
                        stream.write_u64_le(val).await?;
                    }
                    
                    if let Some(meta_text) = meta {
                        let bytes = meta_text.as_bytes();
                        stream.write_u32_le(bytes.len() as u32).await?;
                        stream.write_all(bytes).await?;
                    } else {
                        stream.write_u32_le(0).await?;
                    }
                } else {
                    stream.write_u8(STATUS_ERR).await?;
                }
            }

            OP_DELETE => {
                let payload_id = stream.read_u64_le().await?;
                if state.cluster.delete(payload_id) {
                    stream.write_u8(STATUS_OK).await?;
                } else {
                    stream.write_u8(STATUS_ERR).await?;
                }
            }

            OP_STATS => {
                let cap = state.cluster.total_capacity() as u64;
                let active = state.cluster.total_len() as u64;
                
                stream.write_u8(STATUS_OK).await?;
                stream.write_u64_le(cap).await?;
                stream.write_u64_le(active).await?;
            }

            _ => {
                eprintln!("⚠️ Unknown OpCode received: {}", opcode[0]);
                break; // Sever connection on malformed protocol
            }
        }
    }
    Ok(())
}