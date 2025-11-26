FROM python:3.12-slim

# Work directory inside the container
WORKDIR /app

# Install dependencies needed by the server
RUN pip install --no-cache-dir flask requests

# Copy server application
COPY app.py .

# Default env vars (overridden in docker-compose)
ENV ROLE=follower \
    PORT=8080 \
    WRITE_QUORUM=1 \
    FOLLOWER_URLS="" \
    MIN_DELAY_MS=0.0 \
    MAX_DELAY_MS=1000.0

# Run the key-value store server
CMD ["python", "-u", "app.py"]
