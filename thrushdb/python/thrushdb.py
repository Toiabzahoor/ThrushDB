# python/thrushdb.py

import socket
import struct
import json
import numpy as np
from typing import List, Dict, Optional, Union, Tuple

# Protocol OpCodes
OP_INSERT = 0x01
OP_SEARCH = 0x02
OP_GET    = 0x03
OP_DELETE = 0x04
OP_STATS  = 0x05

STATUS_OK  = 0x00
STATUS_ERR = 0x01

class ThrushDB:
    def __init__(self, host: str = "127.0.0.1", port: int = 3000):
        self.host = host
        self.port = port
        self.conn = None
        self._connect()

    def _connect(self):
        """Establishes a persistent TCP connection to the Rust engine."""
        if self.conn:
            self.conn.close()
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Disable Nagle's algorithm for lower latency
        self.conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.conn.connect((self.host, self.port))

    def _recv_exact(self, n: int) -> bytes:
        """Helper to ensure we read exactly 'n' bytes from the TCP stream."""
        data = bytearray()
        while len(data) < n:
            packet = self.conn.recv(n - len(data))
            if not packet:
                raise ConnectionError("Server closed connection unexpectedly")
            data.extend(packet)
        return bytes(data)

    def _quantize_to_u1024(self, vector: Union[List[float], np.ndarray]) -> List[int]:
        """Converts high-dimensional floats into 16 64-bit integers."""
        if not isinstance(vector, np.ndarray):
            vector = np.array(vector, dtype=np.float32)
        
        # Standardize to 1024 dimensions via tiling/truncating
        if vector.shape[0] < 1024:
            repeats = int(np.ceil(1024 / vector.shape[0]))
            vector = np.tile(vector, repeats)[:1024]
        elif vector.shape[0] > 1024:
            vector = vector[:1024]

        # Sign-bit quantization: > 0 becomes 1, <= 0 becomes 0
        binary_string = "".join(["1" if v > 0 else "0" for v in vector])
        
        # Pack into 16 64-bit chunks
        u64_array = []
        for i in range(16):
            chunk = binary_string[i*64 : (i+1)*64]
            u64_array.append(int(chunk, 2))
            
        return u64_array

    def insert(self, payload_id: int, vector: Union[List[float], np.ndarray], metadata: Union[str, dict] = None) -> bool:
        """Inserts a vector and optional metadata into the database."""
        u1024 = self._quantize_to_u1024(vector)
        
        # Handle metadata
        meta_bytes = b""
        if metadata is not None:
            if isinstance(metadata, dict):
                meta_bytes = json.dumps(metadata).encode('utf-8')
            else:
                meta_bytes = str(metadata).encode('utf-8')
        
        # Build binary packet
        # <B = 1 byte (OpCode)
        # <Q = 8 bytes (Payload ID)
        # 16s = 16 unsigned long longs (128 bytes of Vector)
        # <I = 4 bytes (Metadata Length)
        
        packet = struct.pack("<BQ", OP_INSERT, payload_id)
        for val in u1024:
            packet += struct.pack("<Q", val)
            
        packet += struct.pack("<I", len(meta_bytes))
        packet += meta_bytes

        self.conn.sendall(packet)
        status = self._recv_exact(1)[0]
        return status == STATUS_OK

    def search(self, vector: Union[List[float], np.ndarray], k: int = 5) -> List[Dict]:
        """Searches for the k-nearest neighbors and retrieves their metadata."""
        u1024 = self._quantize_to_u1024(vector)
        
        packet = struct.pack("<B", OP_SEARCH)
        for val in u1024:
            packet += struct.pack("<Q", val)
        packet += struct.pack("<I", k)
        
        self.conn.sendall(packet)
        
        status = self._recv_exact(1)[0]
        if status != STATUS_OK:
            return []

        num_results = struct.unpack("<I", self._recv_exact(4))[0]
        results = []
        
        for _ in range(num_results):
            # Read ID (8) and Distance (4)
            res_id, dist = struct.unpack("<QI", self._recv_exact(12))
            
            # Read Metadata
            meta_len = struct.unpack("<I", self._recv_exact(4))[0]
            meta_str = None
            if meta_len > 0:
                meta_bytes = self._recv_exact(meta_len)
                try:
                    # Try to parse it back into a dict if it was JSON
                    meta_str = json.loads(meta_bytes.decode('utf-8'))
                except json.JSONDecodeError:
                    meta_str = meta_bytes.decode('utf-8')
            
            results.append({
                "payload_id": res_id,
                "distance": dist,
                "metadata": meta_str
            })
            
        return results

    def get(self, payload_id: int) -> Optional[Dict]:
        """Retrieves a vector and its metadata by ID."""
        packet = struct.pack("<BQ", OP_GET, payload_id)
        self.conn.sendall(packet)
        
        status = self._recv_exact(1)[0]
        if status != STATUS_OK:
            return None
            
        # Read the 16 u64s
        vec_data = struct.unpack("<" + "Q"*16, self._recv_exact(128))
        
        # Read Metadata
        meta_len = struct.unpack("<I", self._recv_exact(4))[0]
        meta_str = None
        if meta_len > 0:
            meta_bytes = self._recv_exact(meta_len)
            try:
                meta_str = json.loads(meta_bytes.decode('utf-8'))
            except json.JSONDecodeError:
                meta_str = meta_bytes.decode('utf-8')
                
        return {
            "payload_id": payload_id,
            "vector_bits": list(vec_data),
            "metadata": meta_str
        }

    def delete(self, payload_id: int) -> bool:
        """Deletes a vector and its metadata by ID."""
        packet = struct.pack("<BQ", OP_DELETE, payload_id)
        self.conn.sendall(packet)
        status = self._recv_exact(1)[0]
        return status == STATUS_OK

    def stats(self) -> Dict:
        """Retrieves cluster capacity and active record count."""
        packet = struct.pack("<B", OP_STATS)
        self.conn.sendall(packet)
        
        status = self._recv_exact(1)[0]
        if status != STATUS_OK:
            return {"error": "Failed to fetch stats"}
            
        capacity, active = struct.unpack("<QQ", self._recv_exact(16))
        return {
            "capacity": capacity,
            "active_records": active
        }

    def close(self):
        """Closes the TCP connection."""
        if self.conn:
            self.conn.close()
            self.conn = None