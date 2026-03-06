FROM python:3.11-slim

LABEL maintainer="Japan Culture MCP Team"
LABEL description="Japan Culture MCP Server - 8.3M+ entities knowledge graph"

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies for SQLite FTS5 and R-Tree
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libsqlite3-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml README.md ./
COPY server/ ./server/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Create data directory for volume mount
RUN mkdir -p /app/data

# Default environment
ENV DB_PATH=/app/data/culture_ontology.db
ENV MCP_TRANSPORT=stdio
ENV MCP_HOST=127.0.0.1
ENV PORT=8008

EXPOSE 8008

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=10s \
    CMD curl -sf -o /dev/null -m 3 http://localhost:${PORT}/sse; rc=$?; [ $rc -eq 0 ] || [ $rc -eq 28 ]

ENTRYPOINT ["python", "-m", "server.japan_culture_mcp"]
