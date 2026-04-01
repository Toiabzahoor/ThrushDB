# python/example.py

from thrushdb import ThrushDB
import numpy as np
import time

def main():
    print("Connecting to ThrushDB Binary TCP Server...")
    db = ThrushDB(host="127.0.0.1", port=3000)
    
    # 1. Check Initial Stats
    print("\n[Stats] Pre-Insert:", db.stats())

    # 2. Insert with Metadata!
    print("\n[Insert] Inserting documents with JSON metadata...")
    
    # A generic "physics" concept vector
    physics_vec = np.random.randn(1536) 
    db.insert(
        payload_id=1, 
        vector=physics_vec, 
        metadata={"title": "Introduction to Quantum Mechanics", "author": "Alice"}
    )
    
    # A highly similar physics vector
    physics_vec_2 = physics_vec + (np.random.randn(1536) * 0.1)
    db.insert(
        payload_id=2, 
        vector=physics_vec_2, 
        metadata={"title": "Advanced String Theory", "author": "Bob"}
    )
    
    # A completely different "cooking" vector
    cooking_vec = np.random.randn(1536)
    db.insert(
        payload_id=3, 
        vector=cooking_vec, 
        metadata={"title": "How to Bake a Cake", "author": "Chef John"}
    )

    # 3. Perform a Binary Search
    print("\n[Search] Querying for Physics concepts (k=2):")
    start_time = time.time()
    
    # We search using a vector very close to the first one
    query_vec = physics_vec + (np.random.randn(1536) * 0.05)
    results = db.search(query_vec, k=2)
    
    latency_ms = (time.time() - start_time) * 1000
    
    for rank, res in enumerate(results):
        print(f"  #{rank+1} -> ID: {res['payload_id']} | Dist: {res['distance']} | Meta: {res['metadata']}")
        
    print(f"\n⚡ Search executed in: {latency_ms:.3f} ms over TCP")

    # 4. Clean up
    db.close()

if __name__ == "__main__":
    main()