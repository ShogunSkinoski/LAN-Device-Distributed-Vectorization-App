import socket
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from worker import WorkerNode, TextBatch, TextQuery, WorkerConfig
from discovery import search_and_register

app = FastAPI()
worker = WorkerNode(WorkerConfig())

@app.post("/vectorize")
async def vectorize(batch: TextBatch):
    """Vectorize a batch of texts and store in Milvus"""
    try:
        vectors, milvus_ids = worker.vectorize_batch(batch)
        # Handle NaN values for JSON serialization
        vectors = np.nan_to_num(vectors, nan=0.0, posinf=0.0, neginf=0.0)
        return {
            "batch_id": batch.batch_id,
            "vectors": vectors.tolist(),
            "milvus_ids": milvus_ids,
            "status": "success"
        }
    except Exception as e:
        return {
            "batch_id": batch.batch_id,
            "status": "error",
            "message": str(e)
        }

@app.post("/embed")
async def embed(query: TextQuery):
    """Generate embedding for a single text query"""
    try:
        vector = worker.embed_text(query)
        # Handle NaN values for JSON serialization
        vector = np.nan_to_num(vector, nan=0.0, posinf=0.0, neginf=0.0)
        return {
            "vector": vector.tolist(),
            "status": "success"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate embedding: {str(e)}"
        )

@app.get("/healthz") 
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "cpu_threads": worker.config.cpu_threads,
        "hostname": socket.gethostname()
    }

if __name__ == "__main__":
    search_and_register()
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=20000, workers=1, reload=True)
