FROM python:3.11-slim

LABEL maintainer="Japan Culture MCP Team"
LABEL description="Japan Culture MCP Server - 10M+ entities knowledge graph"

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies for SQLite FTS5 and R-Tree
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY server/ ./server/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Create data directory for volume mount
RUN mkdir -p /app/data

# Default DB path (override via environment variable)
ENV DB_PATH=/app/data/culture_ontology.db

# Expose MCP stdio transport (no port needed for stdio)
# For SSE transport, uncomment:
# EXPOSE 8000

ENTRYPOINT ["python", "-m", "server.japan_culture_mcp"]
