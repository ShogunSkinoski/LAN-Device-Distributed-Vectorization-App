# milvus_client.py
from pymilvus import connections, Collection, DataType, CollectionSchema, FieldSchema
import numpy as np

class Milvus:
    def __init__(self, 
                 host: str = "localhost", 
                 port: str = "19530",
                 collection_name: str = "text_vectors",
                 vector_dim: int = 384):  
        self.collection_name = collection_name
        self.vector_dim = vector_dim
        self.setup_connection(host, port)
        self.setup_collection()

    def setup_connection(self, host: str, port: str):
        try:
            connections.connect(
                alias="default",
                host=host,
                port=port
            )
        except Exception as e:
            print(f"Failed to connect to Milvus: {e}")
            raise

    def setup_collection(self):
        if self.collection_exists():
            self.collection = Collection(self.collection_name)
            return

        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=self.vector_dim)
        ]
        
        schema = CollectionSchema(fields=fields, description="Text embeddings collection")
        self.collection = Collection(name=self.collection_name, schema=schema)
        
        # Create index
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024}
        }
        self.collection.create_index(field_name="embedding", index_params=index_params)

    def collection_exists(self) -> bool:
        from pymilvus import utility
        return utility.has_collection(self.collection_name)

    def insert_batch(self, texts: list[str], vectors: np.ndarray) -> list[int]:
        entities = [
            texts,
            vectors.tolist()
        ]
        
        try:
            insert_result = self.collection.insert(entities)
            self.collection.flush()
            return insert_result.primary_keys
        except Exception as e:
            print(f"Failed to insert batch: {e}")
            raise

    def get_batch(self, vector: np.ndarray, limit: int = 10, expr: str = None) -> tuple[list[str], list[float]]:
        try:
            self.collection.load()
            
            search_params = {
                "metric_type": "L2",
                "params": {"nprobe": 10}
            }
            
            results = self.collection.search(
                data=[vector.tolist()],
                anns_field="embedding",
                param=search_params,
                limit=limit,
                expr=expr,
                output_fields=["text"]
            )
            
            # Extract texts and distances from results
            texts = [hit.entity.get('text') for hit in results[0]]
            distances = [hit.distance for hit in results[0]]
            
            return texts, distances
            
        except Exception as e:
            print(f"Failed to search vectors: {e}")
            raise
        finally:
            self.collection.release()