import socket
from fastapi import FastAPI, HTTPException
import uvicorn
from app.coordinator.coordinator import Coordinator, DiscoveryJob
from typing import Optional
import asyncio

app = FastAPI()
coordinator = Coordinator()

@app.on_event("startup")
async def startup_event():
    """Initialize coordinator when FastAPI starts"""
    await coordinator.start()

@app.post("/register")
async def register(worker_ip: str):
    await coordinator.register_worker(worker_ip)
    return {"status": "success"}

@app.get("/search")
async def search_logs(query: str, limit: Optional[int] = 10):
    """Search for similar log messages"""
    result = await coordinator.distribute_message(
        job="search",
        query=query,
        limit=limit
    )
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result

@app.get("/")
def get_message():
    return "yoo"

@app.get("/semantic")
def get_similiar_logs_by_question(question:str) -> list[str]:
    return coordinator.get_logs(question)

if __name__ == "__main__":
    discovery_job = DiscoveryJob(interval=5)
    discovery_job.start()
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8',80))
        local_ip = s.getsockname()[0]
    finally:
        s.close()
    uvicorn.run("main:app", host=local_ip, port=8081, workers=1, reload=True)
