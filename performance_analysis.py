import json
import time
import subprocess
import threading
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

LEADER_URL = "http://localhost:8080"
NUM_WRITES = 10000
NUM_KEYS = 100
NUM_THREADS = 20

# Global metrics storage 
metrics_latencies = []
metrics_failures = 0
metrics_lock = threading.Lock()


# --------------------------------------------------------------------
# Metrics helpers
# --------------------------------------------------------------------

def add_latency(latency_ms: float) -> None:
    # Record a successful write latency (in ms).
    global metrics_latencies
    with metrics_lock:
        metrics_latencies.append(latency_ms)


def add_failure() -> None:
    # Record a failed write.
    global metrics_failures
    with metrics_lock:
        metrics_failures += 1


def get_stats():
    # Compute summary statistics from collected latencies
    with metrics_lock:
        if not metrics_latencies:
            return None

        latencies = list(metrics_latencies)
        failures = metrics_failures

    # Basic stats
    avg = statistics.mean(latencies)
    median = statistics.median(latencies)
    min_v = min(latencies)
    max_v = max(latencies)

    return {
        "avg": avg,
        "median": median,
        "min": min_v,
        "max": max_v,
        "count": len(latencies),
        "failures": failures,
    }


def reset_metrics() -> None:
    # Reset global metrics before a new run
    global metrics_latencies, metrics_failures
    with metrics_lock:
        metrics_latencies = []
        metrics_failures = 0


def write_key_timed(key: str, value: str):
    
    # PUT a key to the leader and measure latency in ms.
    data = json.dumps({"value": value}).encode("utf-8")
    req = Request(
        f"{LEADER_URL}/kv/{key}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )

    start = time.time()
    try:
        with urlopen(req, timeout=10) as response:
            latency_ms = (time.time() - start) * 1000.0
            result = json.loads(response.read().decode("utf-8"))
            return latency_ms, result
    except HTTPError:
        latency_ms = (time.time() - start) * 1000.0
        return latency_ms, None
    except Exception:
        latency_ms = (time.time() - start) * 1000.0
        return latency_ms, None


def perform_concurrent_writes(num_writes: int, num_keys: int, num_threads: int):
    
    # Perform num_writes PUTs to the leader 
    reset_metrics()

    def write_task(write_id: int) -> None:
        key = f"key{write_id % num_keys}"
        value = f"value_{write_id}"
        latency_ms, result = write_key_timed(key, value)

        if result and result.get("status") == "ok":
            add_latency(latency_ms)
        else:
            add_failure()

    print(f"Starting {num_writes} writes with {num_threads} threads...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(write_task, i) for i in range(num_writes)]

        # Progress tracking
        completed = 0
        for _ in as_completed(futures):
            completed += 1
            if completed % 1000 == 0:
                print(f"  Progress: {completed}/{num_writes}")

    total_time = time.time() - start_time
    stats = get_stats()

    if stats:
        stats["total_time"] = total_time
        stats["throughput"] = num_writes / total_time

    return stats


# --------------------------------------------------------------------
# Consistency check: read from containers
# --------------------------------------------------------------------

def read_from_container(container: str, key: str):
    cmd = f"docker exec {container} curl -s http://localhost:8080/kv/{key}"
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return None
        return None
    except Exception:
        return None


def check_data_consistency(num_keys: int):
    
    # Compare leader data and follower data for all keys key0..key{num_keys-1}.
    # Returns a dict describing missing keys and version mismatches.
    
    print("\n=== Checking Data Consistency ===")

    missing_on_followers = {}
    version_mismatches = {}

    for key_id in range(num_keys):
        key = f"key{key_id}"

        # Read from leader
        leader_data = read_from_container("leader", key)
        if not leader_data:
            continue

        leader_value = leader_data.get("value")
        leader_version = leader_data.get("version")

        # Check followers
        for follower in ["f1", "f2", "f3", "f4", "f5"]:
            follower_data = read_from_container(follower, key)

            if not follower_data:
                missing_on_followers.setdefault(follower, []).append(key)
            elif follower_data.get("version") != leader_version:
                version_mismatches.setdefault(follower, []).append(
                    {
                        "key": key,
                        "leader_version": leader_version,
                        "follower_version": follower_data.get("version"),
                        "leader_value": leader_value,
                        "follower_value": follower_data.get("value"),
                    }
                )

    # Report results
    total_inconsistencies = (
        sum(len(v) for v in missing_on_followers.values())
        + sum(len(v) for v in version_mismatches.values())
    )

    print(f"  Total keys checked: {num_keys}")
    print(f"  Total inconsistencies: {total_inconsistencies}")

    if missing_on_followers:
        print("\n  Missing keys on followers:")
        for follower, keys in missing_on_followers.items():
            print(f"    {follower}: {len(keys)} missing keys")

    if version_mismatches:
        print("\n  Version mismatches:")
        for follower, mismatches in version_mismatches.items():
            print(f"    {follower}: {len(mismatches)} mismatches (showing up to 3)")
            for m in mismatches[:3]:
                print(
                    f"      {m['key']}: "
                    f"leader_v={m['leader_version']} "
                    f"(val={m['leader_value']}), "
                    f"follower_v={m['follower_version']} "
                    f"(val={m['follower_value']})"
                )

    if not missing_on_followers and not version_mismatches:
        print("    All data is consistent across leader and followers!")

    return {
        "missing": missing_on_followers,
        "version_mismatches": version_mismatches,
        "total_inconsistencies": total_inconsistencies,
    }


# --------------------------------------------------------------------
# Quorum handling
# --------------------------------------------------------------------

def update_docker_compose_quorum(quorum: int) -> None:
    # Replace WRITE_QUORUM value in docker-compose.yml.
    with open("docker-compose.yml", "r") as f:
        content = f.read()

    import re

    content = re.sub(r'WRITE_QUORUM: "\d+"', f'WRITE_QUORUM: "{quorum}"', content)

    with open("docker-compose.yml", "w") as f:
        f.write(content)


def restart_with_quorum(quorum: int) -> bool:
    
    # Update WRITE_QUORUM in docker-compose.yml, restart containers, and wait until the leader /health endpoint returns 200.

    print(f"\n{'=' * 60}")
    print(f"Testing with WRITE_QUORUM={quorum}")
    print(f"{'=' * 60}")

    # Update docker-compose.yml
    update_docker_compose_quorum(quorum)

    # Restart containers
    print("Restarting containers with new quorum...")
    subprocess.run(["docker", "compose", "restart"], capture_output=True)

    # Wait for system to be ready
    print("Waiting for system to be ready...")
    time.sleep(3)

    for _ in range(30):
        try:
            with urlopen(f"{LEADER_URL}/health", timeout=1) as resp:
                if resp.status == 200:
                    print(" System ready\n")
                    return True
        except Exception:
            time.sleep(1)

    print("  System not ready")
    return False


# --------------------------------------------------------------------
# Plotting results
# --------------------------------------------------------------------

def plot_results(results_by_quorum: dict) -> None:
    # Plot quorum vs latency 
    try:
        import matplotlib.pyplot as plt

        quorums = sorted(results_by_quorum.keys())
        avg_latencies = [results_by_quorum[q]["avg"] for q in quorums]

        plt.figure(figsize=(10, 6))
        plt.plot(quorums, avg_latencies, marker="o", label="Average Latency", linewidth=2)

        plt.xlabel("Write Quorum Size", fontsize=12)
        plt.ylabel("Latency (ms)", fontsize=12)
        plt.title("Write Latency vs Quorum Size (10K writes, 20 threads)", fontsize=14)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(quorums)

        plt.tight_layout()
        plt.savefig("quorum_vs_latency.png", dpi=300)
        print("\nPlot saved as 'quorum_vs_latency.png'")
        plt.show()

    except ImportError:
        print("\nmatplotlib not installed")
        print("Results summary:")
        for q in sorted(results_by_quorum.keys()):
            print(
                f"  Quorum {q}: "
                f"avg={results_by_quorum[q]['avg']:.2f}ms, "
            )



def run_full_analysis():
    # Run the performance + consistency analysis for quorum=1..5.
    print("=" * 60)
    print("PERFORMANCE ANALYSIS: WRITE QUORUM vs LATENCY")
    print("=" * 60)

    print("\nConfiguration:")
    print(f"  Total writes: {NUM_WRITES}")
    print(f"  Number of keys: {NUM_KEYS}")
    print(f"  Concurrent threads: {NUM_THREADS}")
    print(f"  Writes per key: ~{NUM_WRITES // NUM_KEYS}")

    results_by_quorum = {}
    # Test each quorum value
    for quorum in [1, 2, 3, 4, 5]:
        if not restart_with_quorum(quorum):
            continue

        stats = perform_concurrent_writes(NUM_WRITES, NUM_KEYS, NUM_THREADS)

        if stats:
            results_by_quorum[quorum] = stats

            print(f"\n{'=' * 60}")
            print(f"RESULTS FOR QUORUM={quorum}:")
            print(f"{'=' * 60}")
            print(f"  Total time: {stats['total_time']:.2f}s")
            print(f"  Throughput: {stats['throughput']:.2f} writes/sec")
            print(f"  Successful writes: {stats['count']}")
            print(f"  Failed writes: {stats['failures']}")
            print("\n  Latency Statistics (ms):")
            print(f"    Average: {stats['avg']:.2f}")
            print(f"    Median: {stats['median']:.2f}")
            print(f"    Min: {stats['min']:.2f}")
            print(f"    Max: {stats['max']:.2f}")

        # Wait for replication to settle
        print("\nWaiting for replication to complete...")
        time.sleep(5)

        # Check consistency
        check_data_consistency(NUM_KEYS)

    # Plot results
    if results_by_quorum:
        plot_results(results_by_quorum)

    return results_by_quorum


if __name__ == "__main__":
    run_full_analysis()
