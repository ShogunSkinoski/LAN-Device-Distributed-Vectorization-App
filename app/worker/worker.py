import multiprocessing
from dataclasses import dataclass
from pydantic import BaseModel
import numpy as np
import torch
from sentence_transformers import SentenceTransformer

@dataclass
class WorkerConfig:
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    chunk_size: int = 32
    cpu_threads: int = max(1, multiprocessing.cpu_count() - 1)

class TextBatch(BaseModel):
    texts: list[str]
    batch_id: str

class WorkerNode:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.model = SentenceTransformer(config.model_name)
        self.model.eval()

    def vectorize_batch(self, batch: TextBatch) -> np.ndarray:
        return self.model.encode(batch.texts, batch_size=self.config.chunk_size, show_progress_bar=True)
