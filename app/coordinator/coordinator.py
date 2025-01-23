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
SLAVE_PORT: str = "20000"

@dataclass
class WorkerInfo:
    ip: str
    status: str
    last_heartbeat: float = time.time()

class DiscoveryJob(threading.Thread):
    def __init__(self, interval: int):
        super().__init__()
        self.interval = interval
        self.MC_ADDR = "192.168.1.255"
        self.MC_PORT = 29092
    
    def discover_workers(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(b'Coordinator Discovery', (self.MC_ADDR, self.MC_PORT))
    
    def run(self):
        while True:
            self.discover_workers()
            time.sleep(self.interval)

class MessageJob(threading.Thread):
    def __init__(self, interval: int, loop, topic: str = 'logging.vectorization', bootstrap_servers: str = 'localhost:29092', auto_offset_reset: str = 'latest'):
        super().__init__()
        self.interval = interval
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset
        self.consumer = KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap_servers, auto_offset_reset=self.auto_offset_reset,      enable_auto_commit=True)
        self.message_queue = asyncio.Queue(maxsize=1)
        self.loop = loop  # Main event loop

    def run(self):
        while True:
            for message in self.consumer:
                # Schedule the put in the main loop
                asyncio.run_coroutine_threadsafe(
                    self.message_queue.put(message), 
                    self.loop
                )
            time.sleep(self.interval)

    async def get_message(self):
        return await self.message_queue.get()

class HealthCheckJob(threading.Thread):
    def __init__(self, coordinator, loop):
        super().__init__()
        self.coordinator = coordinator
        self.interval = 5
        self.loop = loop  # Main event loop

    def run(self):
        while True:
            # Schedule health checks on the main loop
            for worker in self.coordinator.workers.copy():  # Avoid iteration issues
                asyncio.run_coroutine_threadsafe(
                    self.check_worker_health(worker),
                    self.loop
                )
            time.sleep(self.interval)

    async def check_worker_health(self, worker: WorkerInfo):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://{worker.ip}:{SLAVE_PORT}/healthz",
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status == 200:
                        worker.status = "active"
                        worker.last_heartbeat = time.time()
                    else:
                        worker.status = "inactive"
        except Exception as e:
            worker.status = "inactive"
            print(f"Health check failed for {worker.ip}: {e}")

class Coordinator:
    def __init__(self):
        self.workers: list[WorkerInfo] = []
        self.milvus = Milvus()
        self.message_job = None
        self.health_check_job = None

    async def start(self):
        """Initialize async components with the main event loop"""
        loop = asyncio.get_running_loop()
        self.message_job = MessageJob(interval=1, loop=loop)
        self.health_check_job = HealthCheckJob(self, loop=loop)
        self.message_job.start()
        self.health_check_job.start()
        asyncio.create_task(self.process_messages())

    async def process_messages(self):
        """Process messages continuously"""
        while True:
            try:
                await self.vectorize_logs()
            except Exception as e:
                print(f"Error processing message: {e}")
            await asyncio.sleep(1)

    async def register_worker(self, worker_ip: str):
        async with aiohttp.ClientSession() as session:
            try:
                print(f"Attempting to register worker at {worker_ip}:{SLAVE_PORT}")
                async with session.get(
                    f"http://{worker_ip}:{SLAVE_PORT}/healthz",
                    timeout=aiohttp.ClientTimeout(total=20)
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
                    
                active_workers = [w for w in self.workers if w.status == "active"]
                print(self.workers)

                if not active_workers:
                    raise ValueError("No active workers available")
                
                worker = active_workers[0]
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"http://{worker.ip}:{SLAVE_PORT}/search",
                        json={"text": kwargs["query"]}
                    ) as response:
                        if response.status != 200:
                            raise Exception(f"Failed to get embedding: {await response.text()}")
                        text_search_result = await response.json()
                        texts = text_search_result["results"]
                        result = {"status": "success", "texts":texts}
                        return result            
            else:
                raise ValueError(f"Unknown job type: {job}")
                
        except Exception as e:
            print(f"Error in distribute_message: {e}")
            return {"status": "error", "message": str(e)}

    async def vectorize_logs(self):
        active_workers = [w for w in self.workers if w.status == "active"]
        if not active_workers:
            return

        message = await self.message_job.get_message()
        message_data = json.loads(message.value.decode('utf-8'))
        
        def extract_log_info(log_dict):
            try:
                log_str = '\n'
                for key in log_dict.keys():
                    if key in ('Id' or 'ApiKeyId'):
                        continue
                    if key == 'Metadata':
                          log_str += "Metadata : {"
                          log_str += extract_log_info(log_dict[key])
                          log_str += '}\n'
                          continue
                    log_str += " " + key + ":" + " " + log_dict[key] + '\n'
                return log_str
            except Exception as e:
                print(f"Error parsing log: {e}")

        processed_text = extract_log_info(message_data)

        worker = active_workers[0]
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://{worker.ip}:{SLAVE_PORT}/vectorize",
                json={"texts": processed_text}
            ) as response:
                if response.status == 200:
                    print(f"Successfully sent batch to worker {worker.ip}")
                else:
                    print(f"Failed to send batch to worker {worker.ip}: {await response.text()}")
