import os
import threading
import time
import random
from typing import Dict, Any, Tuple, List

from flask import Flask, request, jsonify
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================
# Configuration
# ==============================

GLOBAL_VERSION = 0
GLOBAL_VERSION_LOCK = threading.Lock()

ROLE = os.getenv("ROLE", "follower").lower()
PORT = int(os.getenv("PORT", "8080"))

FOLLOWER_URLS_ENV = os.getenv("FOLLOWER_URLS", "")
FOLLOWER_URLS: List[str] = [u.strip() for u in FOLLOWER_URLS_ENV.split(",") if u.strip()]

MIN_DELAY_MS = float(os.getenv("MIN_DELAY_MS", "0.0"))
MAX_DELAY_MS = float(os.getenv("MAX_DELAY_MS", "1000.0"))

DEFAULT_WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "1"))
write_quorum_lock = threading.Lock()
write_quorum = DEFAULT_WRITE_QUORUM

# ==============================
# Data store and concurrency
# ==============================

# STORE: key -> {"value": str, "version": int}
STORE: Dict[str, Dict[str, Any]] = {}
STORE_LOCK = threading.Lock()

# Thread pool for replication tasks on the leader
REPLICATION_EXECUTOR = ThreadPoolExecutor(max_workers=50)

app = Flask(__name__)


# ==============================
# Utility functions
# ==============================

def get_write_quorum() -> int:
    with write_quorum_lock:
        return write_quorum


def set_write_quorum(new_q: int) -> None:
    global write_quorum
    with write_quorum_lock:
        write_quorum = new_q


def set_local_value(key: str, value: str) -> int:
    global GLOBAL_VERSION

    with GLOBAL_VERSION_LOCK:
        GLOBAL_VERSION += 1
        version = GLOBAL_VERSION

    with STORE_LOCK:
        STORE[key] = {"value": value, "version": version}

    print(f"[LEADER] COMMIT key={key} v={version} value={value}")
    return version


def set_local_value_with_version(key: str, value: str, version: int) -> None:

    with STORE_LOCK:
        record = STORE.get(key)
        old_ver = int(record["version"]) if record else 0
        if version >= old_ver:
            STORE[key] = {"value": value, "version": version}
            print(f"[{ROLE.upper()}] APPLY key={key} old_v={old_ver} -> new_v={version} value={value}")

        else:
            print(f"[{ROLE.upper()}] IGNORE key={key} old_v={old_ver} incoming_v={version} value={value}")


def get_local_value(key: str) -> Dict[str, Any]:
    with STORE_LOCK:
        record = STORE.get(key)
        if record is None:
            raise KeyError(key)
        return {"key": key, "value": record["value"], "version": int(record["version"])}


def replicate_to_single_follower(url: str, key: str, value: str, version: int) -> bool:
    try:
        delay_ms = random.uniform(MIN_DELAY_MS, MAX_DELAY_MS)
        time.sleep(delay_ms / 1000.0)

        print(f"[LEADER] send key={key} v={version} to {url} delay={delay_ms:.1f}ms")

        resp = requests.post(
            f"{url}/replicate",
            json={"key": key, "value": value, "version": version},
            timeout=5.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "ok":
                print(f"[LEADER] follower={url} ack={ok} key={key} v={version}")
            return data.get("status") == "ok"
        return False
    except Exception:
        print(f"[LEADER] follower={url} ack={ok} key={key} v={version}")

        return False


def replicate_to_followers(key: str, value: str, version: int) -> Tuple[bool, int]:
    if not FOLLOWER_URLS:
        return True, 0

    futures = []
    for url in FOLLOWER_URLS:
        ftr = REPLICATION_EXECUTOR.submit(
            replicate_to_single_follower, url, key, value, version
        )
        futures.append(ftr)

    required = get_write_quorum()
    ack_count = 0
    success = False

    # Wait only until quorum is reached or all responses have arrived
    for ftr in as_completed(futures):
        try:
            ok = ftr.result()
        except Exception:
            ok = False

        if ok:
            ack_count += 1
            if ack_count >= required:
                success = True
                break

    return success, ack_count


# ==============================
# HTTP API
# ==============================

@app.route("/health", methods=["GET"])
def health() -> Any:
    with STORE_LOCK:
        keys = list(STORE.keys())
    return jsonify({
        "status": "ok",
        "role": ROLE,
        "keys": keys,
        "write_quorum": get_write_quorum(),
    })


@app.route("/set", methods=["POST"])
def handle_set() -> Any:

    if ROLE != "leader":
        return jsonify({"error": "Writes are only accepted on the leader"}), 400

    data = request.get_json(force=True, silent=False)
    key = data.get("key")
    value = data.get("value")

    if key is None or value is None:
        return jsonify({"error": "key and value are required"}), 400

    version = set_local_value(key, value)
    success, ack_count = replicate_to_followers(key, value, version)

    status = "committed" if success else "failed"

    return jsonify({
        "status": status,
        "key": key,
        "value": value,
        "version": version,
        "acks": ack_count,
        "required_quorum": get_write_quorum(),
    }), (200 if success else 500)


@app.route("/replicate", methods=["POST"])
def handle_replicate() -> Any:

    data = request.get_json(force=True, silent=False)
    key = data.get("key")
    value = data.get("value")
    version = data.get("version")

    if key is None or value is None or version is None:
        return jsonify({"error": "key, value and version are required"}), 400

    set_local_value_with_version(key, value, int(version))
    return jsonify({"status": "ok"})


@app.route("/get/<key>", methods=["GET"])
def handle_get(key: str) -> Any:

    try:
        record = get_local_value(key)
        return jsonify(record)
    except KeyError:
        return jsonify({"error": "key not found"}), 404


@app.route("/config/write_quorum", methods=["POST"])
def handle_set_write_quorum() -> Any:

    if ROLE != "leader":
        return jsonify({"error": "write quorum can only be configured on the leader"}), 400

    data = request.get_json(force=True, silent=False)
    q = data.get("write_quorum")
    if q is None:
        return jsonify({"error": "write_quorum is required"}), 400

    q = int(q)
    if q < 1:
        return jsonify({"error": "write_quorum must be >= 1"}), 400

    set_write_quorum(q)
    return jsonify({"status": "ok", "write_quorum": get_write_quorum()})


# ==============================
# Main
# ==============================

if __name__ == "__main__":
    # threaded=True -> concurrent request handling on both leader and followers
    app.run(host="0.0.0.0", port=PORT, threaded=True)
