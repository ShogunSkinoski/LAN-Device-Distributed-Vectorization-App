from fastapi import FastAPI
import uvicorn
from app.coordinator.coordinator import Coordinator, DiscoveryJob

app = FastAPI()
coordinator = Coordinator()


@app.post("/register")
async def register(worker_ip: str = None):
    coordinator.register_worker(worker_ip)
    return {"status": "success"}

@app.get("/")
def get_message():
    coordinator.distribute_message()



if __name__ == "__main__":
    discovery_job = DiscoveryJob(interval=5)
    uvicorn.run("main:app", host="0.0.0.0", port=8081, workers=1, reload=True)
