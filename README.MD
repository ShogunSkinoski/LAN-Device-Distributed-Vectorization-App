# Distributed Text Vectorization Service

A scalable distributed system for text vectorization using FastAPI and sentence transformers. The system consists of a coordinator node that manages multiple worker nodes, which perform the actual text vectorization tasks.

## Architecture

The system follows a coordinator-worker architecture:

- **Coordinator**: Manages worker discovery and registration
- **Workers**: Handle text vectorization using the sentence-transformers model

### Components

- **Coordinator Node**
  - Broadcasts discovery messages using UDP multicast
  - Maintains registry of available workers
  - Provides REST API for worker registration
  - Serves as a load balancer for incoming vectorization requests (will be implemented)

- **Worker Nodes**
  - Listen for coordinator discovery messages
  - Process text vectorization requests
  - Saves vectorized text embeddings to a database (will be implemented)
  - Use sentence-transformers for text embedding
  - Provide health check endpoints

## Features

- Automatic worker discovery using UDP multicast
- Scalable architecture supporting multiple workers
- Fast text vectorization using sentence-transformers
- Health monitoring endpoints
- Configurable worker settings (CPU threads, batch size)
- Database integration for storing vectorized text embeddings (will be implemented)
- REST API for submitting vectorization requests (will be implemented)
