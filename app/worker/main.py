import socket
import numpy as np
from fastapi import FastAPI
from worker import WorkerNode, TextBatch, WorkerConfig
import requests
import time

app = FastAPI()

worker = WorkerNode(WorkerConfig())


@app.post("/vectorize")
async def vectorize(batch: TextBatch):
    try:
        vectors = worker.vectorize_batch(batch)
        vectors = np.nan_to_num(vectors, nan=0.0, posinf=0.0, neginf=0.0)
        return {"batch_id": batch.batch_id, "vectors": vectors.tolist(), "status": "success"}
    except Exception as e:
        return {"batch_id": batch.batch_id, "status": "error", "message": str(e)}
    


@app.get("/health") 
async def health_check():
    return {
        "status": "healthy",
        "cpu_threads": worker.config.cpu_threads,
        "hostname": socket.gethostname()
    }

def search_coordinator():
    MCAST_GRP = '192.168.1.255'
    MCAST_PORT = 25565
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception as e:
        pass
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    sock.bind(('', MCAST_PORT))
    while True:
        print(f"\033[92mSearching for coordinator\033[0m")
        data, addr = sock.recvfrom(1024)
        if data == b"Coordinator Discovery":
            if addr != MCAST_GRP:
                coordinator_ip = addr[0]
                break

        time.sleep(1)
    local_ip = socket.gethostbyname(socket.gethostname())
    requests.post(f"http://{coordinator_ip}:8080/register?worker_ip={local_ip}")
    sock.close()

if __name__ == "__main__":
    search_coordinator()
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=1, reload=True)
