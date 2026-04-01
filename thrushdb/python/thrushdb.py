# python/thrushdb.py

import requests
from typing import List, Dict

def quantize_to_u64_array(float_vector: List[float]) -> List[int]:
    """
    Compresses an array of floats into a 1024-bit binary array represented as 16 u64 integers.
    Uses Sign-Bit Quantization ( >0 becomes 1, <=0 becomes 0 ).
    """
    target_dims = 1024
    
    # Pad or truncate the vector to perfectly match 1024 dimensions
    if len(float_vector) < target_dims:
        float_vector.extend([0.0] * (target_dims - len(float_vector)))
    elif len(float_vector) > target_dims:
        float_vector = float_vector[:target_dims]

    u64_array = []
    
    # Process the 1024 floats in chunks of 64
    for i in range(16):
        chunk = float_vector[i * 64 : (i + 1) * 64]
        
        # Convert each float into a '1' or '0' string
        bit_string = "".join(["1" if f > 0 else "0" for f in chunk])
        
        # Parse the 64-character binary string into a Python integer
        u64_val = int(bit_string, 2)
        u64_array.append(u64_val)
        
    return u64_array

class ThrushClient:
    def __init__(self, host: str = "http://localhost:3000"):
        self.host = host.rstrip("/")

    def stats(self) -> Dict:
        response = requests.get(f"{self.host}/stats")
        response.raise_for_status()
        return response.json()

    def insert(self, payload_id: int, float_vector: List[float]) -> bool:
        binary_vector = quantize_to_u64_array(float_vector)
        
        response = requests.post(f"{self.host}/insert", json={
            "payload_id": payload_id,
            "vector": binary_vector
        })
        return response.status_code == 200

    def search(self, float_vector: List[float], k: int = 5) -> List[Dict]:
        binary_vector = quantize_to_u64_array(float_vector)
        
        response = requests.post(f"{self.host}/search", json={
            "vector": binary_vector,
            "k": k
        })
        response.raise_for_status()
        return response.json()

    def delete(self, payload_id: int) -> bool:
        response = requests.delete(f"{self.host}/delete/{payload_id}")
        return response.status_code == 200