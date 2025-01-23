import asyncio
from dataclasses import dataclass
import time
import threading
import aiohttp
import socket
import json
from app.milvus_client import Milvus
import numpy as np

from kafka import KafkaConsumer
SLAVE_PORT : str = "20000"

@dataclass
class WorkerInfo:
    ip: str
    status: str
    last_heartbeat: float = time.time()

class DiscoveryJob(threading.Thread):
    def __init__(self, interval: int):
        super().__init__(daemon=True) 
        self.interval = interval
        self.MC_ADDR = "224.1.1.1"
        self.MC_PORT = 25565

    def discover_workers(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            sock.sendto(
                b"Coordinator Discovery",  
                (self.MC_ADDR, self.MC_PORT)
            )

    def run(self):
        while True:
            self.discover_workers()
            time.sleep(self.interval)

class MessageJob(threading.Thread):
    def __init__(self, interval: int, topic: str = 'logging.vectorization', bootstrap_servers: str = 'localhost:29092', auto_offset_reset: str = 'earliest'):
        super().__init__()
        self.interval = interval
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset
        self.consumer = KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap_servers, auto_offset_reset=self.auto_offset_reset)
        self.message_queue = asyncio.Queue(maxsize=1)

    def run(self):
        while True:
            for message in self.consumer:
                asyncio.run(self.message_queue.put(message))
            time.sleep(self.interval)

    async def get_message(self):
        return await self.message_queue.get()

class Coordinator:
    def __init__(self):
        self.message_job = MessageJob(interval=1)
        self.workers: list[WorkerInfo] = []
        self.milvus = Milvus()

    async def register_worker(self, worker_ip: str):
        async with aiohttp.ClientSession() as session:
            try:
                print(f"Attempting to register worker at {worker_ip}:{SLAVE_PORT}")
                async with session.get(
                    f"http://{worker_ip}:{SLAVE_PORT}/healthz", 
                    timeout=aiohttp.ClientTimeout(total=5)  # 5 second timeout
                ) as response:
                    response_text = await response.text()
                    print(f"Worker {worker_ip} health check response: {response_text}")
                    
                    if response.status == 200:
                        print(f"Successfully registered worker {worker_ip}")
                        self.workers.append(WorkerInfo(ip=worker_ip, status="active"))
                    else:
                        print(f"Worker {worker_ip} returned non-200 status: {response.status}")
                        self.workers.append(WorkerInfo(ip=worker_ip, status="inactive"))
                    
            except aiohttp.ClientConnectorError as e:
                print(f"Connection error registering worker {worker_ip}: {str(e)}")
                self.workers.append(WorkerInfo(ip=worker_ip, status="inactive"))
            except asyncio.TimeoutError:
                print(f"Timeout registering worker {worker_ip}")
                self.workers.append(WorkerInfo(ip=worker_ip, status="inactive"))
            except Exception as e:
                print(f"Unexpected error registering worker {worker_ip}: {str(e)}")
                self.workers.append(WorkerInfo(ip=worker_ip, status="inactive"))

    async def distribute_message(self, job: str, *args, **kwargs):
        """
        Distribute messages to available workers for vectorization and storage
        
        Args:
            job: Type of job to execute ("vectorize" or "search")
            *args, **kwargs: Additional arguments for specific jobs
        """
        try:
            if job == "vectorize":
                await self.vectorize_logs()
                return {"status": "success", "message": "Vectorization job distributed"}
                
            elif job == "search":
                if "query" not in kwargs:
                    raise ValueError("Search query is required")
                    
                # Find active workers to perform the embedding
                active_workers = [w for w in self.workers if w.status == "active"]
                print(self.workers)

                if not active_workers:
                    raise ValueError("No active workers available")
                
                worker = active_workers[0]
                
                # Get vector embedding from worker
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"http://{worker.ip}:{SLAVE_PORT}/embed",
                        json={"text": kwargs["query"]}
                    ) as response:
                        if response.status != 200:
                            raise Exception(f"Failed to get embedding: {await response.text()}")
                        vector_data = await response.json()
                        vector = vector_data["vector"]
                
                # Search similar vectors in Milvus
                texts, distances = self.milvus.get_batch(
                    vector=np.array(vector),
                    limit=kwargs.get("limit", 10)
                )
                
                return {
                    "status": "success",
                    "results": [
                        {"text": text, "score": float(1 / (1 + distance))}
                        for text, distance in zip(texts, distances)
                    ]
                }
            
            else:
                raise ValueError(f"Unknown job type: {job}")
                
        except Exception as e:
            print(f"Error in distribute_message: {e}")
            return {"status": "error", "message": str(e)}

    async def vectorize_logs(self):
        message = await self.message_job.get_message()
        message_data = json.loads(message.value.decode('utf-8'))
        
        active_workers = [w for w in self.workers if w.status == "active"]
        if not active_workers:
            print("No active workers available")
            return

        # For now, send to first available worker
        # TODO: Implement load balancing
        worker = active_workers[0]
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://{worker.ip}:{SLAVE_PORT}/vectorize",
                json={"texts": message_data["texts"], "batch_id": message_data.get("batch_id", "unknown")}
            ) as response:
                if response.status == 200:
                    print(f"Successfully sent batch to worker {worker.ip}")
                else:
                    print(f"Failed to send batch to worker {worker.ip}: {await response.text()}")
