package pdc;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;

public class DistributedSystemRunner {
    private int masterPort;
    private List<String> workerIds;
    private ProcessLauncher launcher;
    private Socket masterSocket;
    private BufferedReader masterReader;
    private PrintWriter masterWriter;
    private String runtimeToken;
    private Map<String, Long> taskTimings = new ConcurrentHashMap<>();
    private Map<String, String> taskResults = new ConcurrentHashMap<>();
    private List<String> logs = new CopyOnWriteArrayList<>();

    public DistributedSystemRunner(int masterPort, List<String> workerIds) {
        this.masterPort = masterPort;
        this.workerIds = new ArrayList<>(workerIds);
        this.launcher = new ProcessLauncher();
    }

    public void start() throws Exception {
        log("Starting distributed system with " + workerIds.size() + " workers on port " + masterPort);

        launcher.launchMaster(masterPort);

        for (String workerId : workerIds) {
            launcher.launchWorker(workerId, "localhost", masterPort);
        }

        Thread.sleep(2000);
        connectToMaster();
    }

    private void connectToMaster() throws Exception {
        masterSocket = new Socket("localhost", masterPort);
        masterReader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8));
        masterWriter = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8),
                true);

        startResponseListener();
        log("Connected to master on port " + masterPort);
    }

    private void startResponseListener() {
        Thread listener = new Thread(() -> {
            try {
                String line;
                while ((line = masterReader.readLine()) != null) {
                    Message msg = Message.parse(line);

                    if ("TASK_COMPLETE".equals(msg.messageType)) {
                        String[] parts = msg.payload.split(";");
                        String taskId = parts[0];
                        String result = msg.payload.substring(taskId.length() + 1);
                        taskResults.put(taskId, result);
                        taskTimings.put(taskId, System.currentTimeMillis());
                        log("Task completed: " + taskId);
                    }
                }
            } catch (Exception e) {
                log("Listener error: " + e.getMessage());
            }
        });
        listener.setDaemon(true);
        listener.start();
    }

    public String executeTask(String taskId, String taskType, String payload) throws Exception {
        log("Executing task " + taskId + " type=" + taskType);
        taskTimings.put(taskId, System.currentTimeMillis());

        Message req = new Message();
        req.messageType = "RPC_REQUEST";
        req.payload = taskId + ";" + taskType + ";" + payload;
        masterWriter.println(req.toJson());

        long startTime = System.currentTimeMillis();
        while (!taskResults.containsKey(taskId)) {
            if (System.currentTimeMillis() - startTime > 30000) {
                throw new TimeoutException("Task timeout: " + taskId);
            }
            Thread.sleep(100);
        }

        String result = taskResults.get(taskId);
        taskResults.remove(taskId);
        return result;
    }

    public int getWorkerCount() throws Exception {
        Thread.sleep(1000);
        return workerIds.size();
    }

    public void killWorker(String workerId) throws Exception {
        log("Killing worker: " + workerId);

        for (Process p : launcher.getProcesses()) {
            try {
                p.destroyForcibly();
            } catch (Exception e) {
                // ignore
            }
        }

        Thread.sleep(1000);
    }

    public void shutdown() throws Exception {
        log("Shutting down system");
        try {
            masterSocket.close();
        } catch (Exception e) {
            // ignore
        }
        launcher.killAll();
    }

    public List<String> getLogs() {
        return new ArrayList<>(logs);
    }

    private void log(String msg) {
        String logLine = "[" + System.currentTimeMillis() + "] " + msg;
        logs.add(logLine);
        System.out.println(logLine);
    }

    public static void main(String[] args) throws Exception {
        List<String> workers = Arrays.asList("worker-1", "worker-2", "worker-3");
        DistributedSystemRunner runner = new DistributedSystemRunner(9999, workers);

        try {
            runner.start();

            String result = runner.executeTask("test-1", "MATRIX_MULTIPLY", "1,2\\3,4|5,6\\7,8");
            System.out.println("Result: " + result);

            runner.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            runner.shutdown();
        }
    }
}
