import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import requests

LEADER_URL = "http://localhost:8000"
FOLLOWER_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
    "http://localhost:8004",
    "http://localhost:8005",
]


def set_write_quorum(q: int) -> None:
    print(f"\n[config] Setting write quorum to {q}")
    resp = requests.post(
        f"{LEADER_URL}/config/write_quorum",
        json={"write_quorum": q},
        timeout=5.0,
    )
    resp.raise_for_status()


def get_record(base_url: str, key: str) -> Dict:
    resp = requests.get(f"{base_url}/get/{key}", timeout=5.0)
    if resp.status_code != 200:
        raise KeyError(f"{base_url} missing key {key}")
    data = resp.json()
    return {"value": data["value"], "version": int(data["version"])}


def check_key_consistency(key: str) -> None:
    leader_rec = get_record(LEADER_URL, key)
    for url in FOLLOWER_URLS:
        follower_rec = get_record(url, key)
        assert follower_rec == leader_rec, (
            f"[{key}] follower {url} out of sync: {follower_rec} vs {leader_rec}"
        )


def test_single_write_replication() -> None:
    print("\n=== Test 1: single write with quorum=3 ===")
    set_write_quorum(3)

    key = "test_single"
    value = "hello_world"

    resp = requests.post(
        f"{LEADER_URL}/set",
        json={"key": key, "value": value},
        timeout=10.0,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    print("Leader /set response:", data)

    assert data["status"] == "committed"
    assert data["acks"] >= 3, f"Expected at least 3 acks, got {data['acks']}"

    # allow followers to catch up
    time.sleep(3.0)

    # check value + version everywhere
    check_key_consistency(key)
    print(" Test 1 passed: single write replicated consistently to all followers.")


def test_concurrent_writes_consistency() -> None:
    print("\n=== Test 2: concurrent writes with quorum=2 ===")
    set_write_quorum(2)

    keys = [f"it_key_{i}" for i in range(5)]
    values = [f"val_{i}" for i in range(5)]

    def write_key(k: str, v: str):
        r = requests.post(
            f"{LEADER_URL}/set",
            json={"key": k, "value": v},
            timeout=10.0,
        )
        return r.json()

    versions: Dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=len(keys)) as executor:
        futures = []
        for k, v in zip(keys, values):
            futures.append(executor.submit(write_key, k, v))

        for fut in as_completed(futures):
            data = fut.result()
            print("Write result:", data)
            assert data["status"] == "committed"
            k = data["key"]
            versions[k] = int(data["version"])

    # allow replication to catch up
    time.sleep(3.0)

    # check each key on leader + followers
    for k, expected_version in versions.items():
        leader_rec = get_record(LEADER_URL, k)
        expected_value = f"val_{k.split('_')[-1]}"
        assert leader_rec["value"] == expected_value
        assert leader_rec["version"] == expected_version

        for url in FOLLOWER_URLS:
            follower_rec = get_record(url, k)
            assert follower_rec == leader_rec, (
                f"[{k}] follower {url} out of sync: {follower_rec} vs {leader_rec}"
            )

    print(" Test 2 passed: concurrent writes replicated consistently to all followers.")


def main() -> None:
    # quick health check
    print("Checking cluster health...")
    lh = requests.get(f"{LEADER_URL}/health", timeout=5.0).json()
    print("Leader:", lh)
    for f in FOLLOWER_URLS:
        fh = requests.get(f"{f}/health", timeout=5.0).json()
        print(f"{f}:", fh)

    race_demo_same_key()
    print("\n All integration checks PASSED.")


def race_demo_same_key():

    print("\n=== Race demo: 15 concurrent writes on same key ===")
    set_write_quorum(1)  # small quorum to maximize overlap

    key = "race_key"
    num_writes = 15

    def write_once(i: int):
        value = f"race_val_{i}"
        r = requests.post(
            f"{LEADER_URL}/set",
            json={"key": key, "value": value},
            timeout=10.0,
        )
        data = r.json()
        return i, data

    with ThreadPoolExecutor(max_workers=num_writes) as ex:
        futures = [ex.submit(write_once, i) for i in range(num_writes)]
        for fut in as_completed(futures):
            fut.result()

    # Give replication time to finish
    time.sleep(5.0)

    # Final state on leader
    leader_rec = get_record(LEADER_URL, key)
    print("[CLIENT] FINAL leader:", leader_rec)

    # Final state on followers
    for url in FOLLOWER_URLS:
        follower_rec = get_record(url, key)
        print(f"[CLIENT] FINAL {url}:", follower_rec)


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print("\n Integration test FAILED:", e)
        sys.exit(1)
    except Exception as e:
        print("\n Unexpected error:", e)
        sys.exit(1)
