# Raft Distributed Key-Value Store

A production-quality implementation of the Raft consensus algorithm in Java.
Supports leader election, log replication, and fault tolerance.

## Prerequisites

- Java 17+
- Maven 3.6+

## Building

```bash
mvn clean package
```

## Running the Cluster

You need to start 5 terminal windows, one for each node.

**Terminal 1:**
```bash
java -jar target/raft-kv-store.jar node1 8001
```

**Terminal 2:**
```bash
java -jar target/raft-kv-store.jar node2 8002
```

**Terminal 3:**
```bash
java -jar target/raft-kv-store.jar node3 8003
```

**Terminal 4:**
```bash
java -jar target/raft-kv-store.jar node4 8004
```

**Terminal 5:**
```bash
java -jar target/raft-kv-store.jar node5 8005
```

*Note: The second argument (port) is validated against the internal configuration but the configuration dictates the cluster topology.*

## API Usage

Each node exposes an HTTP API on `Port + 1000`.
For `node1` (8001), the API is at `9001`.

**1. Set a Value (Send to Leader)**
If you send to a follower, it might error or redirect (implementation returns 503 if not leader).
Try sending to `node1` (likely leader if started first, or check logs).

```bash
curl -X POST http://localhost:9001/api/set -d '{"key":"name","value":"Gemini"}'
```

**2. Get a Value**
Can be read from any node (eventual consistency).

```bash
curl "http://localhost:9002/api/get?key=name"
# Returns: {"value":"Gemini"}
```

**3. Delete a Value**

```bash
curl -X DELETE "http://localhost:9001/api/delete?key=name"
```

## Testing Fault Tolerance

1. Start all 5 nodes.
2. Set a key.
3. Identify the LEADER from the logs (look for `[nodeX] Became LEADER`).
4. Kill the leader's process (Ctrl+C).
5. Watch the logs of other nodes. One will timeout and start an election.
6. A new leader will be elected.
7. `curl` the new leader or any follower to verify the data is still there.

## Implementation Details

- **Consensus**: Raft (Leader Election, Log Replication)
- **Transport**: gRPC with Protocol Buffers
- **Storage**: In-memory ConcurrentHashMap
- **Language**: Java 17

## Project Structure

- `com.raftdb.raft`: Core Raft logic (Node, Election, Replication)
- `com.raftdb.rpc`: gRPC service implementations and client
- `com.raftdb.storage`: State machine
- `com.raftdb.proto`: Protobuf definitions
