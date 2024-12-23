import asyncio
from dataclasses import dataclass
import time
import threading
import aiohttp
import socket

from kafka import KafkaConsumer

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
        sock.sendto(b'FIND SLAVES TO HANDLE VECTORIZATION', (self.MC_ADDR, self.MC_PORT))

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

    async def register_worker(self, worker_ip: str):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"http://{worker_ip}:8081/register") as response:
                    if response.status == 200:
                        self.workers.append(WorkerInfo(ip=worker_ip, status="active"))
                    else:
                        self.workers.append(WorkerInfo(ip=worker_ip, status="inactive"))
            except Exception as e:
                print(f"Failed to register worker {worker_ip}: {e}")

    def distribute_message(self):
        message = self.message_job.get_message()
        print(message)
        #for worker in self.workers:
        #    if worker.status == "active":
        #        async with aiohttp.ClientSession() as session:
        #            async with session.post(f"http://{worker.ip}:8081/distribute", json={"message": message}) as response:
        #                if response.status == 200:
        #                   print(f"Message distributed to {worker.ip}")
        #                else:
        #                    print(f"Failed to distribute message to {worker.ip}")

