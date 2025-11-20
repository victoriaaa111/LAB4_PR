import os
import json
import threading
import time
import random
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import socket


# ==============================
#  Configuration from env vars
# ==============================

ROLE = os.getenv("ROLE", "follower").lower()        
PORT = int(os.getenv("PORT", "8080"))

WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "1"))
FOLLOWER_URLS_ENV = os.getenv("FOLLOWER_URLS", "")
FOLLOWER_URLS = [u for u in FOLLOWER_URLS_ENV.split(",") if u.strip()]

MIN_DELAY_MS = float(os.getenv("MIN_DELAY_MS", "0.1")) 
MAX_DELAY_MS = float(os.getenv("MAX_DELAY_MS", "1.0"))

# ==============================
#  In memory key-value store
# ==============================

STORE = {}
STORE_LOCK = threading.Lock()
CURRENT_VERSION = 0

# Thread pool for replication tasks
REPL_EXECUTOR = ThreadPoolExecutor(max_workers=32)


# ==============================
#  Helper functions
# ==============================

def log(msg: str):
    print(f"[{ROLE.upper()}] {msg}", flush=True)


def replicate_to_one(follower_url: str, key: str, value, version: int, is_delete: bool = False) -> bool:
    delay_sec = random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000.0
    time.sleep(delay_sec)

    try:
        data = json.dumps({
            "key": key, 
            "value": value, 
            "version": version,
            "is_delete": is_delete
        }).encode('utf-8')
        
        req = Request(
            f"{follower_url}/replicate",
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        
        with urlopen(req, timeout=1.0) as response:
            ok = (response.status == 200)
            if not ok:
                log(f"Replication to {follower_url} FAILED with status {response.status}")
            return ok
            
    except HTTPError as e:
        log(f"Replication to {follower_url} FAILED with status {e.code}")
        return False
    except URLError as e:
        log(f"Replication to {follower_url} EXCEPTION: {e.reason}")
        return False
    except Exception as e:
        log(f"Replication to {follower_url} EXCEPTION: {e}")
        return False


def replicate_to_followers(key: str, value, version: int, is_delete: bool = False) -> int:
    if not FOLLOWER_URLS:
        return 0

    futures = [
        REPL_EXECUTOR.submit(replicate_to_one, url, key, value, version, is_delete)
        for url in FOLLOWER_URLS
    ]

    success = 0
    for ftr in as_completed(futures):
        try:
            if ftr.result():
                success += 1
        except Exception:
            pass

        if success >= WRITE_QUORUM:
            break

    return success


def read_body(rfile, headers):
    length_str = headers.get("Content-Length", "0")
    try:
        length = int(length_str)
    except ValueError:
        length = 0
    
    return rfile.read(length) if length > 0 else b""


def parse_json(body_bytes):
    if not body_bytes:
        return None
    try:
        return json.loads(body_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        return None


def send_response(wfile, status_code, obj):
    body = json.dumps(obj).encode("utf-8")
    
    status_messages = {
        200: "OK",
        400: "Bad Request",
        403: "Forbidden",
        404: "Not Found",
        500: "Internal Server Error"
    }
    
    response_line = f"HTTP/1.1 {status_code} {status_messages.get(status_code, 'OK')}\r\n".encode()
    headers = (
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"\r\n"
    ).encode()
    
    wfile.write(response_line)
    wfile.write(headers)
    wfile.write(body)


# ==============================
#  Route Handlers
# ==============================

def handle_get_key(key, wfile):
    # GET /kv/<key>
    with STORE_LOCK:
        entry = STORE.get(key)

    if entry is None:
        send_response(wfile, 404, {"error": "key not found"})
    else:
        send_response(wfile, 200, {
            "key": key,
            "value": entry["value"],
            "version": entry["version"],
        })


def handle_put_key(key, body_bytes, wfile):
    # PUT /kv/<key> - leader only
    global CURRENT_VERSION

    if ROLE != "leader":
        send_response(wfile, 403, {"error": "writes allowed only on leader"})
        return

    data = parse_json(body_bytes)
    if not data or "value" not in data:
        send_response(wfile, 400, {"error": "invalid json or missing 'value'"})
        return

    value = data["value"]

    # Update leader's store
    with STORE_LOCK:
        CURRENT_VERSION += 1
        version = CURRENT_VERSION
        STORE[key] = {"value": value, "version": version}

    log(f"Received write for key={key}, version={version}, starting replication")

    # Replicate to followers
    success_followers = replicate_to_followers(key, value, version, is_delete=False)

    if success_followers >= WRITE_QUORUM:
        send_response(wfile, 200, {
            "status": "ok",
            "key": key,
            "value": value,
            "version": version,
            "acks": success_followers,
            "required_quorum": WRITE_QUORUM,
        })
    else:
        send_response(wfile, 500, {
            "status": "failed",
            "reason": "not enough follower acknowledgements",
            "acks": success_followers,
            "required_quorum": WRITE_QUORUM,
        })


def handle_delete_key(key, wfile):
    # DELETE /kv/<key> - leader only
    global CURRENT_VERSION

    if ROLE != "leader":
        send_response(wfile, 403, {"error": "deletes allowed only on leader"})
        return

    # Check if key exists
    with STORE_LOCK:
        if key not in STORE:
            send_response(wfile, 404, {"error": "key not found"})
            return
        
        CURRENT_VERSION += 1
        version = CURRENT_VERSION
        # Remove from leader's store
        del STORE[key]

    log(f"Received delete for key={key}, version={version}, starting replication")

    # Replicate delete to followers
    success_followers = replicate_to_followers(key, None, version, is_delete=True)

    if success_followers >= WRITE_QUORUM:
        send_response(wfile, 200, {
            "status": "ok",
            "key": key,
            "version": version,
            "acks": success_followers,
            "required_quorum": WRITE_QUORUM,
        })
    else:
        send_response(wfile, 500, {
            "status": "failed",
            "reason": "not enough follower acknowledgements",
            "acks": success_followers,
            "required_quorum": WRITE_QUORUM,
        })


def handle_replicate(body_bytes, wfile):
    # POST /replicate - followers receive this
    data = parse_json(body_bytes)
    if not data:
        send_response(wfile, 400, {"error": "invalid json"})
        return

    key = data.get("key")
    value = data.get("value")
    version = data.get("version")
    is_delete = data.get("is_delete", False)

    if key is None or version is None:
        send_response(wfile, 400, {"error": "missing key/version"})
        return

    with STORE_LOCK:
        existing = STORE.get(key)
        
        if is_delete:
            # Delete operation
            if existing is None or version >= existing["version"]:
                if key in STORE:
                    del STORE[key]
        else:
            # Write operation
            if "value" not in data:
                send_response(wfile, 400, {"error": "missing value"})
                return
            
            if existing is None or version >= existing["version"]:
                STORE[key] = {"value": value, "version": version}

    send_response(wfile, 200, {"status": "ok"})


# ==============================
#  Request Handler Function
# ==============================

def handle_request(rfile, wfile, request_line, headers):
    """Main request routing function"""
    try:
        parts = request_line.split()
        if len(parts) < 2:
            send_response(wfile, 400, {"error": "bad request"})
            return

        method = parts[0]
        path = parts[1]
        
        parsed = urlparse(path)
        path_parts = parsed.path.strip("/").split("/")

        # Read body for POST/PUT
        body_bytes = b""
        if method in ["POST", "PUT"]:
            body_bytes = read_body(rfile, headers)

        # Route handling
        if method == "GET" and len(path_parts) == 2 and path_parts[0] == "kv":
            handle_get_key(path_parts[1], wfile)
        
        elif method == "PUT" and len(path_parts) == 2 and path_parts[0] == "kv":
            handle_put_key(path_parts[1], body_bytes, wfile)
        
        elif method == "DELETE" and len(path_parts) == 2 and path_parts[0] == "kv":
            handle_delete_key(path_parts[1], wfile)
        
        elif method == "POST" and parsed.path == "/replicate":
            handle_replicate(body_bytes, wfile)
        
        else:
            send_response(wfile, 404, {"error": "not found"})

    except Exception as e:
        log(f"Error handling request: {e}")
        try:
            send_response(wfile, 500, {"error": "internal server error"})
        except:
            pass


# ==============================
#  Simple Server with Threading
# ==============================

def handle_client(conn, addr):
    # Handle a single client connection
    try:
        rfile = conn.makefile('rb')
        wfile = conn.makefile('wb')
        
        # Read request line
        request_line = rfile.readline().decode('utf-8').strip()
        if not request_line:
            return
        
        # Read headers
        headers = {}
        while True:
            line = rfile.readline().decode('utf-8').strip()
            if not line:
                break
            if ':' in line:
                key, value = line.split(':', 1)
                headers[key.strip()] = value.strip()
        
        # Handle the request
        handle_request(rfile, wfile, request_line, headers)
        
        wfile.flush()
        
    except Exception as e:
        log(f"Error with client {addr}: {e}")
    finally:
        try:
            conn.close()
        except:
            pass


def run_server():
    # Run the HTTP server
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("", PORT))
    server_socket.listen(100)
    
    log(f"Starting server on port {PORT}")
    if ROLE == "leader":
        log(f"Role: LEADER, write_quorum={WRITE_QUORUM}, followers={FOLLOWER_URLS}")
    else:
        log("Role: FOLLOWER")
    
    executor = ThreadPoolExecutor(max_workers=50)
    
    try:
        while True:
            conn, addr = server_socket.accept()
            executor.submit(handle_client, conn, addr)
    except KeyboardInterrupt:
        log("Shutting down server")
    finally:
        server_socket.close()
        executor.shutdown(wait=False)
        REPL_EXECUTOR.shutdown(wait=False)


if __name__ == "__main__":
    run_server()