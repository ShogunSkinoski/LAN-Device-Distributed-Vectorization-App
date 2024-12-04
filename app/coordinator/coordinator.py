from datetime import time
import socket
import threading
from fastapi import FastAPI

registry = []
app = FastAPI()

class CronJob(threading.Thread):
    def __init__(self, interval: int):
        super().__init__()
        self.interval = interval
        self.daemon = True
    
    def run(self):
        while True:
            multicast_discovery()
            time.sleep(self.interval)
    

def multicast_discovery():        
    MCAST_GRP = '192.168.1.255'
    MCAST_PORT = 25565
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    sock.sendto(b"Hello World", (MCAST_GRP, MCAST_PORT))

async def lifespan(app: FastAPI):
    cron_job = CronJob(interval=10)
    cron_job.start()
    yield
    cron_job.join()

@app.post("/register/{worker_ip}")
async def register(worker_ip: str):
    registry.append(worker_ip)
    return {"status": "success"}

