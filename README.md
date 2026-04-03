# ThrushDB

ThrushDB is a fast, memory-mapped vector database built from scratch in Rust, featuring a two-level Locality Sensitive Hashing (LSH) engine and a custom binary TCP protocol. I threw this together over 2 days to experiment with high-throughput vector storage and retrieval. 

Currently, it takes high-dimensional float vectors, quantizes them down to 1024-bit binary signatures (`U1024`) via sign-bit quantization, and routes them through a memory-mapped chunk architecture.

## Core Architecture

* **Storage:** Uses `memmap2` for zero-copy disk access. The database divides data into chunks and dynamically allocates overflow files when buckets fill up.
* **Indexing:** Two-level LSH routing. Vectors are hashed into 3 routing tables, and then projected into inner buckets for fast Approximate Nearest Neighbor (ANN) search using Hamming distance.
* **Durability:** Implements a Write-Ahead Log (WAL) and a log-structured append-only metadata store to survive crashes.
* **Protocol:** A custom, ultra-low-latency binary TCP protocol (OpCodes: `INSERT`, `SEARCH`, `GET`, `DELETE`, `STATS`).

## Getting Started

### 1. Run the Rust Server
The backend is a multi-threaded Tokio TCP server listening on port `3000`.

```bash
cargo run --release --bin server
```

### 2. Use the Python Client
The Python client handles the TCP socket connection and automatically quantizes your `numpy` arrays into the 1024-bit format expected by the Rust engine.

```python
from thrushdb import ThrushDB
import numpy as np

# Connect to the TCP server
db = ThrushDB(host="127.0.0.1", port=3000)

# Insert a vector with some JSON metadata
my_vector = np.random.randn(1536)
db.insert(
    payload_id=1, 
    vector=my_vector, 
    metadata={"title": "Quantum Mechanics", "author": "Alice"}
)

# Search for the top 2 nearest neighbors
query_vec = my_vector + (np.random.randn(1536) * 0.05)
results = db.search(query_vec, k=2)

for rank, res in enumerate(results):
    print(f"#{rank+1} -> ID: {res['payload_id']} | Dist: {res['distance']} | Meta: {res['metadata']}")

db.close()
```

## Benchmarks
Here are the results from the built-in performance example testing 200,000 inserts and 20,000 searches. 

```text
$ cargo run --example performance --release

Starting insert phase...
Inserting 200000 vectors...
Insert time: 3.36s
Insert throughput: 59539 IPS

Starting search phase...
Searching 20000 vectors...
Search time: 754.61ms
Search throughput: 26504 QPS
Average search latency: 37.73 µs
```

## What's Next?
Right now, you have to interface directly with the binary TCP server using the provided Python wrapper. I skipped writing a proper HTTP or gRPC API implementation purely out of laziness. 

Honestly, after grinding this out from scratch in two days, I'm pretty bored with it right now. Feel free to clone the repo, run your own benchmarks, and try to break it. If you find errors or figure out ways to crash it, open an issue on GitHub. I'll jump back in and work on improvements once people actually find stuff that needs fixing.
