// src/bin/server.rs

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, delete},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{interval, Duration};

use thrushdb::{ThrushCluster, U1024};

// ─────────────────────────────────────────────────────────────────────────────
// Data Transfer Objects (JSON Schemas)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct InsertRequest {
    payload_id: u64,
    vector: [u64; 16],
}

#[derive(Deserialize)]
struct SearchRequest {
    vector: [u64; 16],
    k: usize,
}

#[derive(Serialize)]
struct SearchResult {
    payload_id: u64,
    distance: u32,
}

#[derive(Serialize)]
struct StatsResponse {
    capacity: usize,
    active_records: usize,
}

#[derive(Serialize)]
struct GetResponse {
    payload_id: u64,
    vector: [u64; 16],
}

// Our shared application state
type SharedCluster = Arc<ThrushCluster>;

// ─────────────────────────────────────────────────────────────────────────────
// HTTP Handlers
// ─────────────────────────────────────────────────────────────────────────────

/// POST /insert
async fn insert_handler(
    State(cluster): State<SharedCluster>,
    Json(payload): Json<InsertRequest>,
) -> impl IntoResponse {
    let vec = U1024(payload.vector);
    
    match cluster.insert(vec, payload.payload_id) {
        Ok(_) => StatusCode::OK,
        Err(e) => {
            eprintln!("Insert failed: {}", e);
            StatusCode::INSUFFICIENT_STORAGE
        }
    }
}

/// POST /search
async fn search_handler(
    State(cluster): State<SharedCluster>,
    Json(payload): Json<SearchRequest>,
) -> impl IntoResponse {
    let vec = U1024(payload.vector);
    let results = cluster.search(vec, payload.k);
    
    let response: Vec<SearchResult> = results
        .into_iter()
        .map(|(id, dist)| SearchResult {
            payload_id: id,
            distance: dist,
        })
        .collect();

    (StatusCode::OK, Json(response))
}

/// GET /get/:id
async fn get_handler(
    State(cluster): State<SharedCluster>,
    Path(payload_id): Path<u64>,
) -> impl IntoResponse {
    match cluster.get(payload_id) {
        Some(vec) => {
            let response = GetResponse {
                payload_id,
                vector: vec.0,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// DELETE /delete/:id
async fn delete_handler(
    State(cluster): State<SharedCluster>,
    Path(payload_id): Path<u64>,
) -> impl IntoResponse {
    if cluster.delete(payload_id) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// GET /stats
async fn stats_handler(State(cluster): State<SharedCluster>) -> impl IntoResponse {
    let stats = StatsResponse {
        capacity: cluster.total_capacity(),
        active_records: cluster.total_len(),
    };
    (StatusCode::OK, Json(stats))
}

// ─────────────────────────────────────────────────────────────────────────────
// Graceful Shutdown & Background Tasks
// ─────────────────────────────────────────────────────────────────────────────

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");
    println!("\n🛑 Received termination signal. Initiating graceful shutdown...");
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Server Boot
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let dir = "data/thrush_live";
    let num_chunks = 15;
    let chunk_capacity = 1_000_000;
    let probe_depth = 64; 

    println!("🔥 BOOTING THRUSH-DB SERVER 🔥");
    println!("Initializing 15-Chunk Memory Mapped Cluster...");

    let cluster = Arc::new(ThrushCluster::new(dir, num_chunks, chunk_capacity, probe_depth).unwrap());
    println!("Cluster Ready. Capacity: {}", cluster.total_capacity());

    // 1. Spawn the Background Auto-Flusher
    // This wakes up every 5 seconds, pushes memory to the physical disk, and goes back to sleep.
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

    // 2. Build the Axum router
    let app = Router::new()
        .route("/insert", post(insert_handler))
        .route("/search", post(search_handler))
        .route("/stats", get(stats_handler))
        .route("/get/:id", get(get_handler))
        .route("/delete/:id", delete(delete_handler))
        .with_state(cluster.clone()); // Clone for Axum state

    // 3. Bind to port 3000
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("🚀 Server listening on http://0.0.0.0:3000");

    // 4. Start the server with the graceful shutdown hook attached
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            println!("💾 Flushing all chunks to disk before exit...");
            if let Err(e) = cluster.flush() {
                eprintln!("❌ Failed to flush during shutdown: {}", e);
            } else {
                println!("✅ Flush complete. Safe to exit.");
            }
        })
        .await
        .unwrap();
}