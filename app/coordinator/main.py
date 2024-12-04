import time
import socket
import threading
from fastapi import FastAPI
import uvicorn

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
    sock.sendto(b"Coordinator Discovery", (MCAST_GRP, MCAST_PORT))

@app.post("/register")
async def register(worker_ip: str = None):
    registry.append(worker_ip)
    return {"status": "success"}

if __name__ == "__main__":
    discovery_job = CronJob(interval=5)
    discovery_job.start()
    uvicorn.run("main:app", host="0.0.0.0", port=8080, workers=1, reload=True)
