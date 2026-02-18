# CSM218 Assignment - Distributed Systems with Java IPC + RPC

## Assignment Overview

Implement a distributed computation system using socket-based Inter-Process Communication (IPC) and a custom Remote Procedure Call (RPC) abstraction. The system must:

- Spawn multiple independent Java processes
- Communicate exclusively via sockets (no RMI, gRPC, or other frameworks)
- Implement a custom RPC protocol for distributed computation
- Execute tasks in parallel across multiple worker processes
- Handle process failures and recovery
- Maintain thread-safe operations

## System Architecture

### Master Process

- Listens on a server socket
- Accepts connections from multiple worker processes
- Distributes computation tasks to workers
- Aggregates results from workers
- Implements heartbeat-based failure detection
- Handles concurrent worker requests

### Worker Processes (≥3)

- Connect to the master process
- Receive distributed computation tasks
- Process tasks (e.g., matrix multiplication)
- Send results back to master
- Respond to heartbeat messages
- Can be killed and restarted

### Message Protocol

All communication must use a structured message format with these required fields:

```json
{
  "magic": "CSM218",
  "version": 1,
  "messageType": "string",
  "studentId": "string",
  "timestamp": 123456789,
  "payload": "string"
}
```

### Message Types

- `CONNECT` - Initial connection to master
- `REGISTER_WORKER` - Worker registration
- `REGISTER_CAPABILITIES` - Worker capability announcement
- `RPC_REQUEST` - Task request from master to worker
- `RPC_RESPONSE` - Response from worker to master
- `TASK_COMPLETE` - Successful task completion
- `TASK_ERROR` - Task execution error
- `HEARTBEAT` - Keep-alive messages
- `WORKER_ACK` - Acknowledgment from master

## Implementation Requirements

### 1. Socket-Based IPC

- Use `ServerSocket` and `Socket` classes
- Implement bidirectional communication
- Handle multiple concurrent connections
- DO NOT use RMI, gRPC, Akka, Netty, or any external RPC framework

### 2. Distributed Computation Task

Implement matrix multiplication:

- Master receives two matrices from input
- Distributes work to available workers
- Workers compute partial results or full matrix product
- Master aggregates results
- Results must be deterministically correct

### 3. Parallelism

- Send multiple tasks simultaneously
- Workers should execute in parallel, not sequentially
- Measure and demonstrate concurrent execution
- System should show speedup with multiple workers

### 4. Concurrency Inside Processes

- Master must handle multiple concurrent worker connections
- Use thread pools or executor services
- Ensure thread-safe data structures
- Handle race conditions properly

### 5. Failure Handling

- Implement heartbeat mechanism (e.g., ping/pong)
- Detect worker failures via heartbeat timeout
- Reassign incomplete tasks to remaining workers
- Continue operation despite worker failure

### 6. Environment Variables

Your code must read configuration from environment variables:

- `STUDENT_ID` - Your student identifier (used in protocol messages)
- `MASTER_PORT` - Port for master process
- `WORKER_ID` - Unique ID for each worker
- `MASTER_HOST` - Hostname/IP of master
- `CSM218_PORT_BASE` - Base port for port allocation

## Implementation Tips

### Main Classes to Implement

**Master.java**

```java
public class Master {
    public Master(int port) { /* initialize */ }
    public void start() { /* start listening */ }
    public void registerWorker(String workerId, Connection conn) { /* add worker */ }
    public String executeTask(String taskType, String payload) { /* distribute and wait */ }
    public void shutdown() { /* cleanup */ }
}
```

**Worker.java**

```java
public class Worker {
    public Worker(String workerId, String masterHost, int masterPort) { /* init */ }
    public void connect() { /* connect to master */ }
    public void processTask(String taskId, String payload) { /* execute computation */ }
    public void run() { /* event loop */ }
}
```

**Message.java**

```java
public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public String payload;

    public String toJson() { /* serialize */ }
    public static Message parse(String json) { /* deserialize */ }
    public void validate() throws Exception { /* verify protocol compliance */ }
}
```

### Protocol Example

**Worker Registration**

1. Worker connects to master socket
2. Worker sends REGISTER_WORKER message with worker ID
3. Master sends WORKER_ACK with runtime token
4. Worker enters listening loop

**Task Execution**

1. Master sends RPC_REQUEST with task ID, type, and payload
2. Worker receives and validates token in request
3. Worker processes task (may involve matrix multiplication)
4. Worker sends TASK_COMPLETE with result
5. Master receives and aggregates result

**Failure Detection**

1. Master periodically sends HEARTBEAT to workers
2. Workers respond with HEARTBEAT ACK
3. If master doesn't receive ACK within timeout, worker is dead
4. Tasks assigned to dead worker are reassigned
5. System continues operation

## Grading Rubric

| Category         | Weight | Criteria                                                 |
| ---------------- | ------ | -------------------------------------------------------- |
| Compilation      | 5%     | Code compiles without errors                             |
| Socket IPC       | 20%    | Bidirectional socket communication, multiple connections |
| RPC Abstraction  | 20%    | Custom RPC protocol, serialization, request/response     |
| Parallelism      | 20%    | Concurrent execution, timing measurements                |
| Distribution     | 15%    | Task distribution, result aggregation                    |
| Failure Handling | 15%    | Heartbeat detection, task reassignment                   |
| Concurrency      | 10%    | Thread-safe operations, concurrent requests              |

**Minimum Passing Score:** 40%

## Testing Your Implementation

### Local Testing

```bash
# Terminal 1: Start master
export MASTER_PORT=9999
export STUDENT_ID="your-id"
java pdc.Master

# Terminal 2-4: Start workers
export WORKER_ID="worker-1"
export MASTER_HOST="localhost"
export MASTER_PORT=9999
java pdc.Worker

# Terminal 5: Test client (if you write one)
java pdc.Client
```

### Run Autograder

```bash
cd autograder
python3 grade.py
```

## Important Constraints

- **NO external RPC libraries** - Only use Java sockets
- **Deterministic computation** - Use fixed matrices for testing
- **Explicit serialization** - Use JSON or DataOutputStream, not string concatenation
- **Environment-based config** - Read ports/IDs from environment variables
- **Proper error handling** - Catch exceptions, report errors to master
- **Thread safety** - Use concurrent collections, synchronized blocks, or locks
- **Logging** - Include informative log messages for debugging

## Submission

Submit your implementation to the GitHub repository:

- Source files in `src/main/java/pdc/`
- Tests in `src/test/java/pdc/`
- Build should pass: `./gradlew build`

The autograder will compile your code, run all tests, and assign a score based on the rubric above.

## Questions?

Contact the course instructor if you have questions about requirements or the autograder.
