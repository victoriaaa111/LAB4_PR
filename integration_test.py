import json
import time
import subprocess
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

LEADER_URL = "http://localhost:8080"


def wait_for_system(max_wait=30):
    print("Waiting for system to be ready...")
    start = time.time()

    while time.time() - start < max_wait:
        try:
            req = Request(f"{LEADER_URL}/health")
            with urlopen(req, timeout=1) as resp:
                if resp.status == 200:
                    print(" System is ready")
                    return True
        except Exception:
            time.sleep(1)

    print(" System did not become ready in time")
    return False


def write_key(key, value):
    # Write a key-value pair to the leader
    data = json.dumps({"value": value}).encode('utf-8')
    req = Request(
        f"{LEADER_URL}/kv/{key}",
        data=data,
        headers={'Content-Type': 'application/json'},
        method='PUT'
    )
    
    try:
        with urlopen(req, timeout=5) as response:
            return json.loads(response.read().decode('utf-8'))
    except HTTPError as e:
        error_body = e.read().decode('utf-8')
        print(f"HTTP Error {e.code}: {error_body}")
        return None
    except Exception as e:
        print(f"Error writing key: {e}")
        return None

def read_key(key):
    # Read a key from the leader
    try:
        with urlopen(f"{LEADER_URL}/kv/{key}", timeout=5) as response:
            return json.loads(response.read().decode('utf-8'))
    except HTTPError as e:
        if e.code == 404:
            return None
        raise

def delete_key(key):
    # Delete a key from the leader
    req = Request(
        f"{LEADER_URL}/kv/{key}",
        method='DELETE'
    )
    
    try:
        with urlopen(req, timeout=5) as response:
            return json.loads(response.read().decode('utf-8'))
    except HTTPError as e:
        if e.code == 404:
            return None
        raise

def read_from_container(container, key):
    # Read a key from a specific container
    cmd = f"docker exec {container} curl -s http://localhost:8080/kv/{key}"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                print(f"  ERROR: Invalid JSON from {container}: {result.stdout[:100]}")
                return None
        else:
            if result.stderr:
                print(f"  ERROR from {container}: {result.stderr[:100]}")
            return None
    except Exception as e:
        print(f"Error reading from {container}: {e}")
        return None

def test_basic_write_read():
    # Test 1: Basic write and read
    print("\n=== Test 1: Basic Write and Read ===")
    
    result = write_key("test1", "value1")
    assert result is not None, "Write failed"
    assert result["status"] == "ok", "Write status not OK"
    assert result["acks"] >= 3, f"Not enough acks: {result['acks']}"
    print(f" Write successful: key=test1, acks={result['acks']}")
    
    data = read_key("test1")
    assert data is not None, "Read failed"
    assert data["value"] == "value1", "Value mismatch"
    print(f" Read successful: {data}")

def test_overwrite():
    # Test 2: Overwrite existing key
    print("\n=== Test 2: Overwrite ===")
    
    write_key("test2", "initial")
    result1 = read_key("test2")
    version1 = result1["version"]
    
    write_key("test2", "updated")
    result2 = read_key("test2")
    version2 = result2["version"]
    
    assert result2["value"] == "updated", "Value not updated"
    assert version2 > version1, "Version not incremented"
    print(f" Overwrite successful: version {version1} → {version2}")

def test_multiple_keys():
    # Test 3: Write multiple keys
    print("\n=== Test 3: Multiple Keys ===")
    
    num_keys = 10
    for i in range(num_keys):
        result = write_key(f"key{i}", f"value{i}")
        assert result is not None, f"Write failed for key{i}"
        assert result["status"] == "ok", f"Write status not OK for key{i}"
    
    print(f" Written {num_keys} keys successfully")
    
    for i in range(num_keys):
        data = read_key(f"key{i}")
        assert data is not None, f"Read failed for key{i}"
        assert data["value"] == f"value{i}", f"Value mismatch for key{i}"
    
    print(f" Read all {num_keys} keys successfully")

def test_delete():
    # Test 4: Delete operation
    print("\n=== Test 4: Delete ===")
    
    # Write a key
    write_key("delete_test", "temporary")
    data = read_key("delete_test")
    assert data is not None, "Key not written"
    print(f" Key written: {data}")
    
    # Delete it
    result = delete_key("delete_test")
    assert result is not None, "Delete failed"
    assert result["status"] == "ok", "Delete status not OK"
    print(f" Delete successful: acks={result['acks']}")
    
    # Verify it's gone
    data = read_key("delete_test")
    assert data is None, "Key still exists after delete"
    print(f" Key successfully deleted")

def test_replication():
    # Test 5: Check replication to followers
    print("\n=== Test 5: Replication ===")
    
    test_key = "replication_test"
    test_value = "replicated_value"
    
    result = write_key(test_key, test_value)
    print(f"  Write result: {result}")
    
    # Wait for replication to complete
    print("  Waiting for replication to settle...")
    time.sleep(3)
    
    # First verify we can read from leader via API
    leader_data = read_key(test_key)
    print(f"  Leader via API: {leader_data}")
    
    containers = ["leader", "f1", "f2", "f3", "f4", "f5"]
    results = {}
    
    print("  Checking containers via docker exec...")
    for container in containers:
        data = read_from_container(container, test_key)
        if data:
            results[container] = data["value"]
            print(f"  {container:8s}: value={data['value']}, version={data['version']}")
        else:
            results[container] = None
            print(f"  {container:8s}: NOT FOUND (curl failed or empty response)")
    
    # Check that at least 4 nodes have the data (leader + 3 followers minimum)
    replicated_count = sum(1 for v in results.values() if v == test_value)
    assert replicated_count >= 4, f"Not enough replicas: {replicated_count}"
    print(f" Data replicated to {replicated_count} nodes")

def test_version_consistency():
    # Test 6: Version numbers increase monotonically
    print("\n=== Test 6: Version Consistency ===")
    
    versions = []
    for i in range(5):
        result = write_key("version_test", f"v{i}")
        data = read_key("version_test")
        versions.append(data["version"])
    
    # Check versions are strictly increasing
    for i in range(1, len(versions)):
        assert versions[i] > versions[i-1], f"Version not increasing: {versions}"
    
    print(f" Versions are monotonically increasing: {versions}")

def test_concurrent_writes():
    # Test 7: Concurrent writes to same key
    print("\n=== Test 7: Concurrent Writes ===")
    
    from concurrent.futures import ThreadPoolExecutor
    
    def write_task(i):
        return write_key("concurrent_test", f"value_{i}")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(write_task, range(5)))
    
    # All should succeed
    success_count = sum(1 for r in results if r and r.get("status") == "ok")
    print(f" {success_count}/5 concurrent writes succeeded")
    
    # Final value should be from one of the writes
    data = read_key("concurrent_test")
    assert data is not None, "Key not found after concurrent writes"
    assert data["value"].startswith("value_"), "Invalid final value"
    print(f" Final value: {data['value']}, version: {data['version']}")

def run_all_tests():
    """Run all integration tests"""
    print("=" * 60)
    print("INTEGRATION TESTS FOR KEY-VALUE STORE")
    print("=" * 60)
    
    if not wait_for_system():
        print(" System not ready.")
        return False
    
    print(" System is ready\n")
    
    try:
        test_basic_write_read()
        test_overwrite()
        test_multiple_keys()
        test_delete()
        test_replication()
        test_version_consistency()
        test_concurrent_writes()
        
        print("\n" + "=" * 60)
        print("!! ALL TESTS PASSED !!")
        print("=" * 60)
        return True
        
    except AssertionError as e:
        print(f"\n!!! TEST FAILED: {e} !!!")
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"\n ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)