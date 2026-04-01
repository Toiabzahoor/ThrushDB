# python/real_world_ai.py

import os
import time
import requests
from thrushdb import ThrushClient

# We use the free Hugging Face Inference API. 
# It runs the heavy AI model on their servers and returns the float array.
HF_API_URL = "https://router.huggingface.co/hf-inference/models/BAAI/bge-large-en-v1.5"
HF_TOKEN = os.environ.get("HF_TOKEN") # We will set this in the terminal

def get_ai_embeddings(texts: list[str]) -> list[list[float]]:
    """Sends text to Hugging Face and gets back 1024-dimensional float arrays."""
    if not HF_TOKEN:
        raise ValueError("Please set the HF_TOKEN environment variable.")
        
    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    response = requests.post(HF_API_URL, headers=headers, json={"inputs": texts})
    
    if response.status_code != 200:
        raise Exception(f"Hugging Face API Error: {response.text}")
        
    return response.json()

def main():
    db = ThrushClient("http://localhost:3000")
    
    print("🔥 BOOTING REAL-WORLD AI INTEGRATION 🔥")
    
    # 1. Our Knowledge Base
    # We will map these strings to payload_ids (0, 1, 2, 3)
    knowledge_base = {
        100: "The Rust programming language focuses on memory safety and fearless concurrency.",
        101: "Photosynthesis is the process by which plants use sunlight to synthesize foods from carbon dioxide and water.",
        102: "Vector databases store data as mathematical coordinates in high-dimensional space.",
        103: "A black hole is a region of spacetime where gravity is so strong that nothing can escape."
    }
    
    print("\n🧠 1. Fetching AI Embeddings from Hugging Face...")
    start_time = time.time()
    
    texts_to_embed = list(knowledge_base.values())
    
    # This returns four arrays, each containing 1024 floats
    float_embeddings = get_ai_embeddings(texts_to_embed) 
    
    print(f"   ✅ Received {len(float_embeddings)} embeddings in {time.time() - start_time:.2f} seconds.")
    
    print("\n💾 2. Quantizing and Inserting into ThrushDB...")
    for payload_id, float_vector in zip(knowledge_base.keys(), float_embeddings):
        db.insert(payload_id=payload_id, float_vector=float_vector)
        print(f"   -> Inserted Document {payload_id} into Rust Engine.")

    # 3. The Real-World Query
    search_query = "How do vector databases map semantic meaning?"
    print(f"\n🔍 3. Searching for: '{search_query}'")
    
    # Get the embedding for our query
    query_vector = get_ai_embeddings([search_query])[0]
    
    # Search our Rust DB
    db_start = time.time()
    results = db.search(float_vector=query_vector, k=2)
    db_time = (time.time() - db_start) * 1000
    
    print(f"   ⏱️  ThrushDB Search Time: {db_time:.2f} ms")
    print("\n🎯 TOP RESULTS:")
    
    for rank, result in enumerate(results):
        matched_id = result["payload_id"]
        distance = result["distance"]
        text = knowledge_base.get(matched_id, "Unknown Document")
        print(f"   #{rank + 1} | Distance: {distance} | Text: {text}")

if __name__ == "__main__":
    main()