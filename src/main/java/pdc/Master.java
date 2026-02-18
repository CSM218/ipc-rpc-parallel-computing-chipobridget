package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * Handles worker registration, RPC request distribution, heartbeat monitoring,
 * failure detection with timeout, and task recovery/reassignment.
 */
public class Master {

    private ServerSocket serverSocket;
    private int port;
    private String studentId;

    private final ExecutorService systemThreads = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(10, r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    private final ConcurrentHashMap<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> workerHeartbeat = new ConcurrentHashMap<>();

    private final BlockingQueue<TaskUnit> pendingTasks = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, TaskUnit> activeTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> taskToWorker = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> taskResults = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean acceptingConnections = false;

    private static final long HEARTBEAT_TIMEOUT_MS = 5000;
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private final AtomicInteger taskCounter = new AtomicInteger(0);

    public Master() {
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null) {
            try {
                this.port = Integer.parseInt(portEnv);
            } catch (NumberFormatException e) {
                this.port = 9999;
            }
        } else {
            this.port = 9999;
        }
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) {
            this.studentId = "student_default";
        }
    }

    public Master(int port) {
        this.port = port;
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) {
            this.studentId = "student_default";
        }
    }

    /**
     * Entry point for distributed computation coordination.
     * Partitions work, schedules across workers, handles aggregation.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (workers.isEmpty()) {
            return null;
        }

        List<String> workerIds = new ArrayList<>(workers.keySet());
        int numWorkers = Math.min(workerCount, workerIds.size());
        int rows = data.length;
        int rowsPerWorker = Math.max(1, rows / numWorkers);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numWorkers && i * rowsPerWorker < rows; i++) {
            int startRow = i * rowsPerWorker;
            int endRow = (i == numWorkers - 1) ? rows : (i + 1) * rowsPerWorker;
            String taskId = "coord_" + taskCounter.getAndIncrement();
            String wId = workerIds.get(i % workerIds.size());

            StringBuilder sb = new StringBuilder();
            sb.append(startRow).append(",").append(endRow).append(";");
            for (int r = startRow; r < endRow; r++) {
                for (int c = 0; c < data[r].length; c++) {
                    if (c > 0) sb.append(",");
                    sb.append(data[r][c]);
                }
                sb.append(";");
            }

            String payload = sb.toString();
            TaskUnit task = new TaskUnit(taskId, operation, payload, startRow, endRow);
            activeTasks.put(taskId, task);

            futures.add(taskExecutor.submit(() -> {
                try {
                    sendRpcRequest(wId, taskId, operation, payload);
                } catch (IOException e) {
                    System.err.println("Failed to send task " + taskId + ": " + e.getMessage());
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("Task submission error: " + e.getMessage());
            }
        }

        return null;
    }

    /**
     * Start the communication listener on the specified port.
     * Uses ServerSocket with background accept loop - returns immediately.
     */
    public void listen(int port) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.port = serverSocket.getLocalPort();
        this.running.set(true);
        this.acceptingConnections = true;
        System.out.println("Master listening on port " + this.port);

        systemThreads.submit(this::monitorHeartbeats);
        systemThreads.submit(this::acceptConnections);
    }

    /**
     * Background loop to accept incoming worker connections.
     */
    private void acceptConnections() {
        while (running.get() && acceptingConnections) {
            try {
                Socket workerSocket = serverSocket.accept();
                systemThreads.submit(() -> handleWorkerConnection(workerSocket));
            } catch (SocketException e) {
                if (running.get()) {
                    System.err.println("ServerSocket error: " + e.getMessage());
                }
                break;
            } catch (IOException e) {
                if (running.get()) {
                    System.err.println("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Handles a single worker connection: registration, RPC request/response loop.
     */
    private void handleWorkerConnection(Socket socket) {
        String workerId = null;
        try {
            DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            Message regMsg = Message.readFromStream(dis);
            regMsg.validate();

            if ("REGISTER_WORKER".equals(regMsg.messageType)) {
                workerId = regMsg.payload;
                if (workerId == null || workerId.isEmpty()) {
                    workerId = regMsg.studentId;
                }

                WorkerConnection conn = new WorkerConnection(workerId, socket, dis, dos);
                workers.put(workerId, conn);
                workerHeartbeat.put(workerId, System.currentTimeMillis());
                System.out.println("Worker registered: " + workerId);

                Message ack = new Message("WORKER_ACK", studentId);
                ack.payload = "REGISTERED";
                ack.writeToStream(dos);

                processWorkerMessages(workerId, conn);
            }
        } catch (EOFException e) {
            // Connection closed normally
        } catch (IOException e) {
            if (running.get()) {
                System.err.println("Worker connection error: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Unexpected error handling worker: " + e.getMessage());
        } finally {
            if (workerId != null) {
                handleWorkerFailure(workerId);
            }
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Processes incoming messages from a connected worker in an event loop.
     */
    private void processWorkerMessages(String workerId, WorkerConnection conn) throws IOException {
        while (running.get()) {
            try {
                Message msg = Message.readFromStream(conn.dis);
                if (msg == null) break;

                switch (msg.messageType) {
                    case "TASK_COMPLETE":
                        handleTaskComplete(workerId, msg);
                        break;
                    case "HEARTBEAT":
                        workerHeartbeat.put(workerId, System.currentTimeMillis());
                        break;
                    case "TASK_ERROR":
                        handleTaskError(workerId, msg);
                        break;
                    case "RPC_RESPONSE":
                        handleRpcResponse(workerId, msg);
                        break;
                    default:
                        System.out.println("Unknown messageType from " + workerId + ": " + msg.messageType);
                }
            } catch (EOFException e) {
                break;
            }
        }
    }

    /**
     * Handles task completion result from a worker.
     */
    private void handleTaskComplete(String workerId, Message msg) {
        String taskPayload = msg.payload;
        if (taskPayload != null) {
            String[] parts = taskPayload.split(";", 2);
            String taskId = parts[0];
            String result = parts.length > 1 ? parts[1] : "";
            taskResults.put(taskId, result);
            activeTasks.remove(taskId);
            taskToWorker.remove(taskId);
            System.out.println("Task " + taskId + " completed by " + workerId);
        }
    }

    /**
     * Handles task error - queues for retry via recovery mechanism.
     */
    private void handleTaskError(String workerId, Message msg) {
        String taskPayload = msg.payload;
        if (taskPayload != null) {
            String[] parts = taskPayload.split(";", 2);
            String taskId = parts[0];
            TaskUnit task = activeTasks.remove(taskId);
            if (task != null) {
                pendingTasks.offer(task);
                System.out.println("Task " + taskId + " failed, queued for retry");
            }
        }
    }

    /**
     * Handles an RPC response from a worker.
     */
    private void handleRpcResponse(String workerId, Message msg) {
        taskResults.put(workerId + "_rpc", msg.payload);
    }

    /**
     * Sends an RPC request to a specific worker.
     */
    public void sendRpcRequest(String workerId, String taskId, String taskType, String taskPayload) throws IOException {
        WorkerConnection conn = workers.get(workerId);
        if (conn == null) {
            throw new IOException("Worker not found: " + workerId);
        }
        Message rpcRequest = new Message("RPC_REQUEST", studentId);
        rpcRequest.payload = taskId + ";" + taskType + ";" + taskPayload;
        synchronized (conn.dos) {
            rpcRequest.writeToStream(conn.dos);
        }
        taskToWorker.put(taskId, workerId);
    }

    /**
     * System Health Check - reconciles cluster state.
     * Detects dead workers via heartbeat timeout and triggers task recovery.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();
        List<String> deadWorkers = new ArrayList<>();

        for (Map.Entry<String, Long> entry : workerHeartbeat.entrySet()) {
            if (now - entry.getValue() > HEARTBEAT_TIMEOUT_MS) {
                deadWorkers.add(entry.getKey());
            }
        }

        for (String deadId : deadWorkers) {
            System.out.println("Detected dead worker (heartbeat timeout): " + deadId);
            handleWorkerFailure(deadId);
        }
    }

    /**
     * Handles worker failure: removes worker and triggers task recovery/reassignment.
     */
    private void handleWorkerFailure(String workerId) {
        WorkerConnection conn = workers.remove(workerId);
        workerHeartbeat.remove(workerId);
        if (conn != null) {
            try {
                conn.socket.close();
            } catch (IOException ignored) {
            }
        }
        recoverAndReassignTasks(workerId);
    }

    /**
     * Recovery mechanism: reassigns tasks from a failed worker to available workers.
     * Implements retry logic for tasks that cannot be immediately redistributed.
     */
    private void recoverAndReassignTasks(String failedWorkerId) {
        List<String> tasksToReassign = new ArrayList<>();

        for (Map.Entry<String, String> entry : taskToWorker.entrySet()) {
            if (failedWorkerId.equals(entry.getValue())) {
                tasksToReassign.add(entry.getKey());
            }
        }

        if (tasksToReassign.isEmpty()) return;

        System.out.println("Recovering " + tasksToReassign.size() + " tasks from failed worker: " + failedWorkerId);
        List<String> availableWorkers = new ArrayList<>(workers.keySet());

        if (availableWorkers.isEmpty()) {
            for (String taskId : tasksToReassign) {
                TaskUnit task = activeTasks.get(taskId);
                if (task != null) {
                    pendingTasks.offer(task);
                    System.out.println("No workers available, queued task " + taskId + " for retry");
                }
            }
            return;
        }

        int workerIdx = 0;
        for (String taskId : tasksToReassign) {
            TaskUnit task = activeTasks.get(taskId);
            if (task == null) continue;

            String targetWorker = availableWorkers.get(workerIdx % availableWorkers.size());
            workerIdx++;

            taskExecutor.submit(() -> {
                try {
                    sendRpcRequest(targetWorker, taskId, task.operation, task.payload);
                    taskToWorker.put(taskId, targetWorker);
                    System.out.println("Reassigned task " + taskId + " to worker " + targetWorker);
                } catch (IOException e) {
                    pendingTasks.offer(task);
                    System.err.println("Reassignment failed for task " + taskId + ", queued for retry");
                }
            });
        }
    }

    /**
     * Monitors worker heartbeats and sends periodic health check pings.
     * Detects failures via timeout mechanism and triggers recovery.
     */
    private void monitorHeartbeats() {
        while (running.get()) {
            try {
                Thread.sleep(HEARTBEAT_INTERVAL_MS);
                long now = System.currentTimeMillis();
                List<String> deadWorkers = new ArrayList<>();

                for (Map.Entry<String, Long> entry : workerHeartbeat.entrySet()) {
                    if (now - entry.getValue() > HEARTBEAT_TIMEOUT_MS) {
                        deadWorkers.add(entry.getKey());
                    }
                }

                for (String deadId : deadWorkers) {
                    System.out.println("Heartbeat timeout detected for worker: " + deadId);
                    handleWorkerFailure(deadId);
                }

                for (WorkerConnection conn : workers.values()) {
                    try {
                        Message ping = new Message("HEARTBEAT", studentId);
                        ping.payload = "PING";
                        synchronized (conn.dos) {
                            ping.writeToStream(conn.dos);
                        }
                    } catch (IOException e) {
                        // Will be caught by timeout check
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Shuts down the Master and all connections.
     */
    public void shutdown() {
        running.set(false);
        acceptingConnections = false;

        for (WorkerConnection conn : workers.values()) {
            try {
                conn.socket.close();
            } catch (IOException ignored) {
            }
        }
        workers.clear();

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }

        systemThreads.shutdownNow();
        taskExecutor.shutdownNow();
    }

    /**
     * Internal class representing a worker connection.
     */
    private static class WorkerConnection {
        final String workerId;
        final Socket socket;
        final DataInputStream dis;
        final DataOutputStream dos;

        WorkerConnection(String workerId, Socket socket, DataInputStream dis, DataOutputStream dos) {
            this.workerId = workerId;
            this.socket = socket;
            this.dis = dis;
            this.dos = dos;
        }
    }

    /**
     * Internal class representing a computational task unit.
     */
    private static class TaskUnit {
        final String id;
        final String operation;
        final String payload;
        final int startRow;
        final int endRow;

        TaskUnit(String id, String operation, String payload, int startRow, int endRow) {
            this.id = id;
            this.operation = operation;
            this.payload = payload;
            this.startRow = startRow;
            this.endRow = endRow;
        }
    }

    /**
     * Main entry point for running the Master process.
     */
    public static void main(String[] args) {
        String portStr = System.getenv("MASTER_PORT");
        int masterPort = 9999;
        if (portStr != null) {
            try {
                masterPort = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                System.err.println("Invalid MASTER_PORT, using default 9999");
            }
        }

        Master master = new Master(masterPort);
        Runtime.getRuntime().addShutdownHook(new Thread(master::shutdown));

        try {
            master.listen(masterPort);
            System.out.println("Master started on port " + masterPort);
            Thread.currentThread().join();
        } catch (Exception e) {
            System.err.println("Master error: " + e.getMessage());
            master.shutdown();
        }
    }
}
