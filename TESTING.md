# Strict Ordering Test Case Verification

This document explains how to verify the "Pause-Aware" architecture's **Strict Per-Key Ordering** ($Old Buffered Events \rightarrow New Events$) using both quick Unit Tests and full End-to-End Integration Tests.

## Test Objective
To validate that the system enforces ordering guarantees even under extreme concurrency. Specifically, when a "Resume" command and "New Events" arrive at the same millisecond, the system must **always** drain the buffer first.

## The Worst-Case Scenario
Both tests simulate the following sequence on a key (e.g., `123`):
1.  **Normal**: 3 initial events sent (Status: ACTIVE).
2.  **Buffering**: Key PAUSED. 500 events sent. (Expect: 0 output, 500 buffered).
3.  **Race Condition**: Key RESUMED + 2 New Events sent immediately.
4.  **Verification**: 
    -   Total Output: 505 events.
    -   Order: [3 Active] -> [500 Buffered] -> [2 New].

---

## 1. Unit Test (Recommended for Logic)
**Type**: JUnit + TopologyTestDriver  
**Location**: `backend/src/test/java/com/example/app/topology/StrictOrderingTest.java`  
**Why**: Deterministic, fast, no Docker required. Mathematically proves the processor logic handles the race condition.

### How to Run
```bash
cd backend
./gradlew test --tests com.example.app.topology.StrictOrderingTest
```

### Success Output
```
=== START TEST: Strict Ordering Worst Case Scenario ===
Step 1: Initial State (Active)
...
Step 3: Resume Race Condition Simualtion
...
   Last Buffered Event: b499 (Expected: b499)
   First New Event: n1 (Expected: n1)
   Second New Event: n2 (Expected: n2)

=== SUCCESS: Strict Ordering Verified! ===
BUILD SUCCESSFUL
```

---

## 2. Integration Test (Recommended for Scalability)
**Type**: Python Script + Running Docker Cluster  
**Location**: `scripts/tests/verify_strict_ordering.py`  
**Why**: Verifies the entire stack (Controller API, Kafka Broker, Serialization, Network Latency, and Client timings).

### Prerequisites
1.  Python 3 installed.
2.  The application stack MUST be running (`./run_compose.sh`).

### How to Run with Virtual Environment
It is best practice to run Python scripts in an isolated environment.

1.  **Ensure Stack is Running**
    ```bash
    ./run_compose.sh
    ```

2.  **Setup Python Environment**
    ```bash
    # Create a virtual environment named 'venv'
    python3 -m venv venv

    # Activate the virtual environment
    # On macOS/Linux:
    source venv/bin/activate
    # On Windows:
    # venv\Scripts\activate

    # Install dependencies
    pip install requests
    ```

3.  **Run the Verification Script**
    ```bash
    python3 scripts/tests/verify_strict_ordering.py
    ```

### Success Output
```
Starting End-to-End Strict Ordering Verification for Key: strict-ordering-test-ab12cd34
[1] Sending 3 initial events (Active)...
[2] Pausing key...
[3] Sending 500 events to buffer...
   Buffer verified: 500 events.
[4] Attempting Race Condition (Resume + 2 New Events)...
[6] Fetching Output Messages...
[6.1] Found 505 messages for key...
[7] Verifying Strict Ordering...

SUCCESS: Strict Ordering Verified via Integration Test!
```

---

## Comparison
| Feature | Unit Test | Integration Test |
| :--- | :--- | :--- |
| **Speed** | Instant (<2s) | Slow (~10s + Setup) |
| **Docker Required?** | No | Yes |
| **Logic Verification**| **Perfect** (Deterministic) | **Good** (Probabilistic) |
| **Network Issues** | Ignored | Covered |
| **Serialization** | Mocked | Real (Over-the-wire) |

**Recommendation**: Use **Unit Tests** for development loops and CI. Use **Integration Tests** before major releases or deployment.
