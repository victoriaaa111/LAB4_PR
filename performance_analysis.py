import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple

import requests
import matplotlib.pyplot as plt

LEADER_URL = "http://localhost:8000"
FOLLOWER_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
    "http://localhost:8004",
    "http://localhost:8005",
]

TOTAL_WRITES = 100
CONCURRENCY = 5
KEYS = [f"key_{i}" for i in range(10)]  # 10 keys: key_0..key_9


def set_write_quorum(q: int) -> None:
    resp = requests.post(
        f"{LEADER_URL}/config/write_quorum",
        json={"write_quorum": q},
        timeout=5.0,
    )
    resp.raise_for_status()


def do_write(session: requests.Session, key: str, value: str) -> Tuple[float, Dict]:

    start = time.time()
    resp = session.post(
        f"{LEADER_URL}/set",
        json={"key": key, "value": value},
        timeout=10.0,
    )
    latency_ms = (time.time() - start) * 1000.0
    data = resp.json()
    return latency_ms, data


def run_experiment_for_quorum(q: int) -> float:

    print(f"\n=== Running experiment for write quorum = {q} ===")
    set_write_quorum(q)
    time.sleep(1.0)  # small pause to let config settle

    latencies: List[float] = []
    successes = 0

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = []
            for i in range(TOTAL_WRITES):
                key = KEYS[i % len(KEYS)]
                value = f"value_q{q}_{i}"
                futures.append(executor.submit(do_write, session, key, value))

            for fut in as_completed(futures):
                try:
                    latency_ms, data = fut.result()
                    latencies.append(latency_ms)
                    if data.get("status") == "committed":
                        successes += 1
                    else:
                        print("Write failed:", data)
                except Exception as e:
                    print("Error during write:", e)

    if not latencies:
        raise RuntimeError("No write latencies collected")

    avg_latency = statistics.mean(latencies)
    print(
        f"Quorum {q}: average latency = {avg_latency:.2f} ms, "
        f"successes = {successes}/{TOTAL_WRITES}"
    )
    return avg_latency


def get_record(base_url: str, key: str) -> Dict:
    resp = requests.get(f"{base_url}/get/{key}", timeout=5.0)
    if resp.status_code != 200:
        raise KeyError(f"{base_url} missing key {key}")
    data = resp.json()
    return {"value": data["value"], "version": int(data["version"])}


def check_data_consistency(label: str = "") -> None:

    if label:
        print(f"\n=== Consistency check {label} ===")
    else:
        print("\n=== Consistency check ===")

    # Give some time for lagging replication calls to finish
    time.sleep(5.0)

    mismatches = []

    for key in KEYS:
        try:
            leader_rec = get_record(LEADER_URL, key)
        except KeyError:
            print(f"Leader is missing key {key}")
            mismatches.append((key, "leader-missing"))
            continue

        for follower in FOLLOWER_URLS:
            try:
                follower_rec = get_record(follower, key)
            except KeyError:
                print(f"{follower} is missing key {key}")
                mismatches.append((key, f"{follower}-missing"))
                continue

            if follower_rec != leader_rec:
                print(
                    f"Mismatch for key {key} on {follower}: "
                    f"leader={leader_rec}, follower={follower_rec}"
                )
                mismatches.append((key, f"{follower}-mismatch"))

    if not mismatches:
        print("All replicas are consistent with the leader (value + version).")
    else:
        print(f"Found {len(mismatches)} inconsistencies.")


def main() -> None:
    quorums = [1, 2, 3, 4, 5]
    avg_latencies = []

    # Warmup health check
    print("Checking cluster health...")
    print("Leader:", requests.get(f"{LEADER_URL}/health", timeout=5.0).json())
    for f in FOLLOWER_URLS:
        print(f"{f}:", requests.get(f"{f}/health", timeout=5.0).json())

    for q in quorums:
        avg = run_experiment_for_quorum(q)
        avg_latencies.append(avg)

        check_data_consistency(label=f"after write_quorum={q}")

    # Plot write_quorum vs average latency
    plt.figure()
    plt.plot(quorums, avg_latencies, marker="o")
    plt.xlabel("Write Quorum")
    plt.ylabel("Average Write Latency (ms)")
    plt.title("Write Quorum vs Average Latency (semi-synchronous replication)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("write_quorum_vs_latency.png")
    plt.show()


if __name__ == "__main__":
    main()
