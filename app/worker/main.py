import socket
import numpy as np
from fastapi import FastAPI, Request
from torch import cosine_similarity
from worker import WorkerNode, TextBatch, WorkerConfig
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=1, reload=True)
