from fastapi import FastAPI, HTTPException
import uvicorn
from app.coordinator.coordinator import Coordinator, DiscoveryJob
from typing import Optional

app = FastAPI()
coordinator = Coordinator()

@app.post("/register")
async def register(worker_ip: str):
    await coordinator.register_worker(worker_ip)
    return {"status": "success"}

@app.post("/vectorize")
async def vectorize_logs():
    """Trigger vectorization of new log messages"""
    result = await coordinator.distribute_message(job="vectorize")
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result

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
    uvicorn.run("main:app", host="0.0.0.0", port=8081, workers=1, reload=True)
