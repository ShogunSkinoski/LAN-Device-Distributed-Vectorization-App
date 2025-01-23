# worker.py
from dataclasses import dataclass
from typing import Optional, Union
import multiprocessing
from pydantic import BaseModel
import numpy as np
from sentence_transformers import SentenceTransformer
from app.milvus_client import Milvus

@dataclass
class WorkerConfig:
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    chunk_size: int = 32
    cpu_threads: int = max(1, multiprocessing.cpu_count() - 1)
    milvus_host: str = "localhost"
    milvus_port: str = "19530"

class TextBatch(BaseModel):
    texts: list[str]
    batch_id: str

class TextQuery(BaseModel):
    text: str

class WorkerNode:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.model = SentenceTransformer(config.model_name)
        self.model.eval()
        self.milvus = Milvus(
            host=config.milvus_host,
            port=config.milvus_port
        )

    def vectorize_batch(self, batch: TextBatch) -> tuple[np.ndarray, Optional[list[int]]]:
        """Vectorize a batch of texts and store in Milvus"""
        vectors = self.model.encode(
            batch.texts, 
            batch_size=self.config.chunk_size, 
            show_progress_bar=True
        )
        
        try:
            milvus_ids = self.milvus.insert_batch(batch.texts, vectors)
            return vectors, milvus_ids
        except Exception as e:
            print(f"Failed to write to Milvus: {e}")
            return vectors, None

    def embed_text(self, query: Union[str, TextQuery]) -> np.ndarray:
        """Generate embedding for a single text query"""
        if isinstance(query, TextQuery):
            text = query.text
        else:
            text = query
            
        vector = self.model.encode(
            text,
            batch_size=1,
            show_progress_bar=False
        )
        return vector
    
    def get_similiar_texts(self,vector, limit=10):
        # Search similar vectors in Milvus
        texts, distances = self.milvus.get_batch(
            vector=np.array(vector),
            limit=limit
        )
        
        return {
            "status": "success",
            "results": [
                {"text": text, "score": float(1 / (1 + distance))}
                for text, distance in zip(texts, distances)
            ]
        }

