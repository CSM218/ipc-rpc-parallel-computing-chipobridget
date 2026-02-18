package pdc;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * Connects to Master, processes RPC requests, responds to heartbeat health checks,
 * and supports failure recovery with timeout detection.
 */
public class Worker {

    private String workerId;
    private String masterHost;
    private int masterPort;
    private String studentId;
    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, Long> taskTimings = new ConcurrentHashMap<>();
    private static final long HEARTBEAT_TIMEOUT_MS = 5000;

    public Worker() {
        this.workerId = System.getenv("WORKER_ID");
        if (this.workerId == null) {
            this.workerId = "worker_" + System.currentTimeMillis();
        }
        this.masterHost = System.getenv("MASTER_HOST");
        if (this.masterHost == null) {
            this.masterHost = "localhost";
        }
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null) {
            try {
                this.masterPort = Integer.parseInt(portEnv);
            } catch (NumberFormatException e) {
                this.masterPort = 9999;
            }
        } else {
            this.masterPort = 9999;
        }
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) {
            this.studentId = "student_default";
        }
    }

    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) {
            this.studentId = "student_default";
        }
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * Handles connection failure gracefully with timeout.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            socket.setSoTimeout((int) HEARTBEAT_TIMEOUT_MS);
            inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            Message regMsg = new Message("REGISTER_WORKER", studentId);
            regMsg.payload = workerId;
            regMsg.writeToStream(outputStream);

            Message ackMsg = Message.readFromStream(inputStream);
            if ("WORKER_ACK".equals(ackMsg.messageType)) {
                System.out.println("Worker " + workerId + " registered successfully");
                running.set(true);
            } else {
                System.err.println("Unexpected response: " + ackMsg.messageType);
            }
        } catch (ConnectException e) {
            System.err.println("Could not connect to master at " + masterHost + ":" + port + " - " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Failed to join cluster: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error joining cluster: " + e.getMessage());
        }
    }

    /**
     * Main execution loop - processes RPC requests from Master.
     * Responds to heartbeat health checks and executes tasks concurrently.
     */
    public void execute() {
        if (inputStream == null || !running.get()) {
            return;
        }

        try {
            while (running.get()) {
                try {
                    Message msg = Message.readFromStream(inputStream);
                    if (msg == null) break;

                    String msgType = msg.messageType;

                    if ("RPC_REQUEST".equals(msgType)) {
                        taskExecutor.submit(() -> processRpcRequest(msg));
                    } else if ("HEARTBEAT".equals(msgType)) {
                        Message pong = new Message("HEARTBEAT", studentId);
                        pong.payload = "PONG";
                        synchronized (outputStream) {
                            pong.writeToStream(outputStream);
                        }
                    } else if ("MATRIX_BLOCK_MULTIPLY".equals(msgType)) {
                        taskExecutor.submit(() -> processMatrixTask(msg));
                    }
                } catch (EOFException e) {
                    System.out.println("Connection closed by master");
                    break;
                } catch (java.net.SocketTimeoutException e) {
                    continue;
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                System.err.println("Error in execution loop: " + e.getMessage());
            }
        } finally {
            cleanup();
        }
    }

    /**
     * Processes an RPC request from the Master.
     */
    private void processRpcRequest(Message requestMsg) {
        try {
            String payload = requestMsg.payload;
            String[] parts = payload.split(";", 3);
            if (parts.length < 3) return;

            String taskId = parts[0];
            String taskType = parts[1];
            String taskData = parts[2];

            taskTimings.put(taskId, System.currentTimeMillis());

            String result;
            if ("MATRIX_MULTIPLY".equals(taskType)) {
                result = executeMatrixMultiply(taskData);
            } else {
                result = "UNSUPPORTED_TASK";
            }

            Message response = new Message("TASK_COMPLETE", studentId);
            response.payload = taskId + ";" + result;
            synchronized (outputStream) {
                response.writeToStream(outputStream);
            }
            System.out.println("Completed RPC request: " + taskId);
        } catch (Exception e) {
            try {
                Message errorMsg = new Message("TASK_ERROR", studentId);
                errorMsg.payload = "ERROR:" + e.getMessage();
                synchronized (outputStream) {
                    errorMsg.writeToStream(outputStream);
                }
            } catch (IOException ioe) {
                System.err.println("Failed to send error response: " + ioe.getMessage());
            }
        }
    }

    /**
     * Processes a matrix block multiplication task.
     */
    private void processMatrixTask(Message taskMsg) {
        try {
            String payload = taskMsg.payload;
            String[] parts = payload.split(";");
            if (parts.length < 2) return;

            String[] metadata = parts[0].split(",");
            int startRow = Integer.parseInt(metadata[0]);
            int endRow = Integer.parseInt(metadata[1]);
            int colsB = Integer.parseInt(metadata[2]);
            int innerDim = Integer.parseInt(metadata[3]);
            int numRows = endRow - startRow;

            int[][] matrixA = new int[numRows][];
            int idx = 1;
            for (int i = 0; i < numRows && idx < parts.length; i++) {
                String[] values = parts[idx++].split(",");
                matrixA[i] = new int[values.length];
                for (int j = 0; j < values.length; j++) {
                    matrixA[i][j] = Integer.parseInt(values[j]);
                }
            }

            int[][] matrixB = new int[innerDim][];
            for (int i = 0; i < innerDim && idx < parts.length; i++) {
                String[] values = parts[idx++].split(",");
                matrixB[i] = new int[values.length];
                for (int j = 0; j < values.length; j++) {
                    matrixB[i][j] = Integer.parseInt(values[j]);
                }
            }

            int[][] result = new int[numRows][colsB];
            for (int i = 0; i < numRows; i++) {
                for (int j = 0; j < colsB; j++) {
                    for (int k = 0; k < innerDim; k++) {
                        result[i][j] += matrixA[i][k] * matrixB[k][j];
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append(startRow).append(",").append(endRow).append(",").append(colsB).append(";");
            for (int i = 0; i < numRows; i++) {
                for (int j = 0; j < result[i].length; j++) {
                    if (j > 0) sb.append(",");
                    sb.append(result[i][j]);
                }
                sb.append(";");
            }

            Message resultMsg = new Message("TASK_COMPLETE", studentId);
            resultMsg.payload = sb.toString();
            synchronized (outputStream) {
                resultMsg.writeToStream(outputStream);
            }
        } catch (Exception e) {
            System.err.println("Matrix task error: " + e.getMessage());
        }
    }

    /**
     * Executes matrix multiplication from string-encoded matrices.
     * Format: "row1,row2\row3,row4|col1,col2\col3,col4"
     */
    private String executeMatrixMultiply(String data) {
        try {
            String[] matrices = data.split("\\|");
            if (matrices.length < 2) return "ERROR:INVALID_DATA";

            int[][] matA = parseMatrix(matrices[0]);
            int[][] matB = parseMatrix(matrices[1]);

            int rowsA = matA.length;
            int colsA = matA[0].length;
            int colsB = matB[0].length;

            int[][] result = new int[rowsA][colsB];
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    for (int k = 0; k < colsA; k++) {
                        result[i][j] += matA[i][k] * matB[k][j];
                    }
                }
            }

            return matrixToString(result);
        } catch (Exception e) {
            return "ERROR:" + e.getMessage();
        }
    }

    private int[][] parseMatrix(String matStr) {
        String[] rows = matStr.split("\\\\");
        int[][] matrix = new int[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            String[] vals = rows[i].split(",");
            matrix[i] = new int[vals.length];
            for (int j = 0; j < vals.length; j++) {
                matrix[i][j] = Integer.parseInt(vals[j].trim());
            }
        }
        return matrix;
    }

    private String matrixToString(int[][] matrix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) sb.append(",");
                sb.append(matrix[i][j]);
            }
            if (i < matrix.length - 1) sb.append("\\");
        }
        return sb.toString();
    }

    /**
     * Cleanup resources on shutdown.
     */
    private void cleanup() {
        running.set(false);
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
        taskExecutor.shutdownNow();
    }

    /**
     * Main entry point for running the Worker process.
     */
    public static void main(String[] args) {
        String workerId = System.getenv("WORKER_ID");
        if (workerId == null) {
            workerId = "worker_" + System.currentTimeMillis();
        }
        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null) {
            masterHost = "localhost";
        }
        int masterPort = 9999;
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null) {
            try {
                masterPort = Integer.parseInt(portEnv);
            } catch (NumberFormatException e) {
                System.err.println("Invalid MASTER_PORT, using default 9999");
            }
        }

        Worker worker = new Worker(workerId, masterHost, masterPort);
        Runtime.getRuntime().addShutdownHook(new Thread(worker::cleanup));
        worker.joinCluster(masterHost, masterPort);
    }
}
