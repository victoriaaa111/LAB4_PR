# LAB 4 PR: Key-Value Store with Single-Leader Replication

During this laboratory work I implemented a distributed key-value store that uses a single-leader replication model. The system runs one leader and multiple follower replicas in separate Docker containers, all communicating through a simple HTTP interface.

## Contents of Directory
* I have multiple files in the root. The *integration_test.py* and *performance_analysis.py* files  are used for testing the server and client functionality and plotting the results with different `quorum` values. The *app.py* file contains the functionality for both followers and leader. *report_images* directory contains images used in this report.
* In this project, the *Dockerfile* prepares a minimal Python environment used by both the leader and follower containers, installing the server code and configuring how each node starts. The *docker-compose.yml* file launches one leader and five follower containers, sets their roles through environment variables, connects them on the same network, and exposes the leader’s port so clients and tests can access the system.

![img_12.png](report_images%2Fimg_12.png)

## Dockerfile

This *Dockerfile* builds a lightweight `Python 3.12` container that runs either a leader or follower node of the key-value store. It installs `curl`, copies the server code into the container, and sets default environment variables that control the node’s role, port, quorum size, and simulated network delay. When the container starts, it simply executes `app.py`, which reads these environment variables to determine how the node behaves.

```dockerfile
FROM python:3.12-slim

# Work directory inside the container
WORKDIR /app

#install curl
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

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
```

## Docker compose

This *docker-compose.yml* file launches the entire distributed system by creating six containers: one leader and five followers, all built from the same Docker image. It assigns each container its role, port, follower list, quorum size, and delay parameters through environment variables, and connects them on the same internal network.

```dockerfile
services:
  leader:
    build: .
    container_name: leader
    environment:
      ROLE: "leader"
      PORT: "8080"
      FOLLOWER_URLS: "http://f1:8080,http://f2:8080,http://f3:8080,http://f4:8080,http://f5:8080"
      WRITE_QUORUM: "3"
      # simulate network lag
      MIN_DELAY_MS: "0.1"
      MAX_DELAY_MS: "1.0"
    ports:
      - "8080:8080"  # expose leader to host
    depends_on:
      - f1
      - f2
      - f3
      - f4
      - f5

  f1:
    build: .
    container_name: f1
    environment:
      ROLE: "follower"
      PORT: "8080"
    expose:
      - "8080"

  f2:
    build: .
    container_name: f2
    environment:
      ROLE: "follower"
      PORT: "8080"
    expose:
      - "8080"

  f3:
    build: .
    container_name: f3
    environment:
      ROLE: "follower"
      PORT: "8080"
    expose:
      - "8080"

  f4:
    build: .
    container_name: f4
    environment:
      ROLE: "follower"
      PORT: "8080"
    expose:
      - "8080"

  f5:
    build: .
    container_name: f5
    environment:
      ROLE: "follower"
      PORT: "8080"
    expose:
      - "8080"

```

## Running the project

We can run the project using docker with the following command and it will start all 6 containers:
```
docker compose up 
```
What we get in the terminal after we run:

![img_13.png](report_images%2Fimg_13.png)

## Integration testing
I have created an integration test script *integration_test.py* that performs a series of tests to verify the functionality of the key-value store. The tests include basic put and get operations, quorum enforcement, replication and consistency across all stores.

We can run the tests using the following command:
```
python3 integration_test.py
```
Here is the output of the tests and we can see that all tests have passed successfully:

![img_14.png](report_images%2Fimg_14.png)

![img_15.png](report_images%2Fimg_15.png)

## Performance analysis

I have created a performance analysis script *performance_analysis.py* that measures the latency of put operations under different quorum sizes. The script runs 10000 requests to the leader node and records the time taken for each operation. It then plots the average latency against different quorum sizes.

We can run the performance analysis using the following command:
``` 
python3 performance_analysis.py
```

And we get the following output showing the average latency for different quorum sizes:

![quorum_vs_latency.png](report_images%2Fquorum_vs_latency.png)

We also get the following output in the terminal showing the average and median latency values for each quorum size, as well as checks for consistency:

![img.png](report_images%2Fimg.png)

![img_1.png](report_images%2Fimg_1.png)

![img_2.png](report_images%2Fimg_2.png)

![img_3.png](report_images%2Fimg_3.png)

![img_4.png](report_images%2Fimg_4.png)

![img_5.png](report_images%2Fimg_5.png)

![img_6.png](report_images%2Fimg_6.png)

![img_7.png](report_images%2Fimg_7.png)

![img_8.png](report_images%2Fimg_8.png)

![img_9.png](report_images%2Fimg_9.png)

## Sending request using curl
We can also send requests to the leader using the curl command:

![img_10.png](report_images%2Fimg_10.png)

And here are the logs in the terminal that confirm the correct number of quorum and that the write was successful:

![img_11.png](report_images%2Fimg_11.png)