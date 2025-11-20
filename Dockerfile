FROM python:3.12-slim

# Work directory inside the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requests

# Copy server code
COPY app.py .

# Default env vars (can be overridden in docker-compose)
ENV PORT=8080
ENV ROLE=follower
ENV WRITE_QUORUM=3
ENV FOLLOWER_URLS=""
ENV MIN_DELAY_MS=0.1
ENV MAX_DELAY_MS=1.0

# Start the Python HTTP server
CMD ["python", "app.py"]
