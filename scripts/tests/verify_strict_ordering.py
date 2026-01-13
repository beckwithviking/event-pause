
import requests
import time
import uuid

# Configuration
BASE_URL = "http://localhost:8080/control"
TOPIC_ID = "demo"
KEY = "strict-ordering-test-" + str(uuid.uuid4())[:8]

def log(step, message):
    print(f"[{step}] {message}")

def check_response(resp, context):
    if resp.status_code >= 400:
        print(f"FAILED: {context} - {resp.status_code} - {resp.text}")
        exit(1)

def main():
    print(f"Starting End-to-End Strict Ordering Verification for Key: {KEY}")

    # 1. Initial State: Send 3 events
    log("1", "Sending 3 initial events (Active)...")
    for i in range(3):
        resp = requests.post(f"{BASE_URL}/{TOPIC_ID}/send", params={"key": KEY, "data": f"active-{i}"})
        check_response(resp, f"Send initial event {i}")
    time.sleep(1) # Allow processing time

    # 2. Pause
    log("2", f"Pausing key {KEY}...")
    resp = requests.post(f"{BASE_URL}/{TOPIC_ID}/pause/{KEY}")
    check_response(resp, "Pause Key")
    time.sleep(1) # Ensure status propagates

    # 3. Buffer 500 Events
    log("3", "Sending 500 events to buffer...")
    for i in range(500):
        # We use a session for performance if needed, but requests is fine for 500
        resp = requests.post(f"{BASE_URL}/{TOPIC_ID}/send", params={"key": KEY, "data": f"buffered-{i}"})
        if i % 100 == 0:
            print(f"   Sent {i} events...")
    
    # 3.1 Verify Buffer
    log("3.1", "Verifying buffer size...")
    resp = requests.get(f"{BASE_URL}/{TOPIC_ID}/buffer/{KEY}")
    data = resp.json()
    if len(data) != 500:
        print(f"WARNING: Buffer size is {len(data)}, expected 500. Processing might lag or pause failed.")
    else:
        print("   Buffer verified: 500 events.")

    # 4. Trigger Race Condition: Resume + New Events
    log("4", "Attempting Race Condition (Resume + 2 New Events)...")
    
    # We want these to happen as close as possible.
    # While Python requests are synchronous, we send them back-to-back.
    requests.post(f"{BASE_URL}/{TOPIC_ID}/resume/{KEY}")
    requests.post(f"{BASE_URL}/{TOPIC_ID}/send", params={"key": KEY, "data": "new-race-1"})
    requests.post(f"{BASE_URL}/{TOPIC_ID}/send", params={"key": KEY, "data": "new-race-2"})
    
    log("5", "Waiting for processing...")
    time.sleep(5) # Wait for all processing to complete

    # 5. Verify Output
    log("6", "Fetching Output Messages...")
    # NOTE: The API /output-messages gets recent messages from the topic. 
    # Since we can't filter by Key in the API comfortably (it consumes recent 50), 
    # we might need to fetch a large number or use a Kafka consumer directly if this fails.
    # Let's try fetching 1000 messages and filtering.
    resp = requests.get(f"{BASE_URL}/{TOPIC_ID}/output-messages", params={"limit": 1000})
    check_response(resp, "Fetch Output")
    
    messages = resp.json()
    
    # The output consumer returns a flat structure:
    # { "key": "...", "eventId": "...", "data": "...", ... }
    
    filtered_msgs = []
    for m in messages:
         if m.get('key') == KEY:
             filtered_msgs.append(m)
    
    log("6.1", f"Found {len(filtered_msgs)} messages for key {KEY}")

    if len(filtered_msgs) < 505:
        print(f"ERROR: Expected 505 messages, found {len(filtered_msgs)}.")
        # Print last few to see what happened
        print("Last 5 messages found:")
        for m in filtered_msgs[-5:]:
            print(m)
        exit(1)

    # 6. Verify Order
    log("7", "Verifying Strict Ordering...")
    
    # Expected Sequence:
    # 0-2: active-0..2
    # 3-502: buffered-0..499
    # 503: new-race-1
    # 504: new-race-2
    
    errors = 0
    
    # Check Active
    for i in range(3):
        actual_data = filtered_msgs[i]['data']
        expected = f"active-{i}"
        if actual_data != expected:
            print(f"Mismatch at index {i}: Expected {expected}, got {actual_data}")
            errors += 1

    # Check Buffered
    for i in range(500):
        idx = 3 + i
        actual_data = filtered_msgs[idx]['data']
        expected = f"buffered-{i}"
        if actual_data != expected:
            print(f"Mismatch at index {idx}: Expected {expected}, got {actual_data}")
            errors += 1
            if errors > 5: break

    # Check New
    if filtered_msgs[503]['data'] != "new-race-1":
        print(f"Mismatch at 503: Expected new-race-1, got {filtered_msgs[503]['data']}")
        errors += 1
    
    if filtered_msgs[504]['data'] != "new-race-2":
        print(f"Mismatch at 504: Expected new-race-2, got {filtered_msgs[504]['data']}")
        errors += 1

    if errors == 0:
        print("\nSUCCESS: Strict Ordering Verified via Integration Test!")
        print("-" * 50)
        print("Visual Confirmation (Last 5 Messages):")
        for i, m in enumerate(filtered_msgs[-5:]):
            # Displaying index 500 to 504 (inclusive)
            idx = 500 + i 
            print(f"[{idx}] Data: {m.get('data')} | EventId: {m.get('eventId')}")
        print("-" * 50)
    else:
        print(f"\nFAILURE: Found {errors} ordering errors.")

if __name__ == "__main__":
    main()
