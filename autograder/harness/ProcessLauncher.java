package pdc;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class ProcessLauncher {
    private List<Process> processes = new CopyOnWriteArrayList<>();

    public Process launchMaster(int port) throws IOException {
        String[] cmd = {
                "java",
                "-cp", getClasspath(),
                "pdc.ReferenceMaster"
        };

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("MASTER_PORT", String.valueOf(port));
        pb.environment().put("STUDENT_ID", "autograder");
        pb.redirectErrorStream(true);

        Process p = pb.start();
        processes.add(p);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return p;
    }

    public Process launchWorker(String workerId, String masterHost, int masterPort) throws IOException {
        String[] cmd = {
                "java",
                "-cp", getClasspath(),
                "pdc.ReferenceWorker"
        };

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("WORKER_ID", workerId);
        pb.environment().put("MASTER_HOST", masterHost);
        pb.environment().put("MASTER_PORT", String.valueOf(masterPort));
        pb.environment().put("STUDENT_ID", "autograder");
        pb.redirectErrorStream(true);

        Process p = pb.start();
        processes.add(p);

        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return p;
    }

    public void killAll() {
        for (Process p : processes) {
            try {
                p.destroyForcibly();
            } catch (Exception e) {
                // ignore
            }
        }
        processes.clear();
    }

    public static String getClasspath() {
        return ".:" + System.getProperty("java.class.path");
    }

    public static void main(String[] args) throws Exception {
        ProcessLauncher launcher = new ProcessLauncher();

        Process master = launcher.launchMaster(9999);
        System.out.println("Master started");

        Process w1 = launcher.launchWorker("worker-1", "localhost", 9999);
        Process w2 = launcher.launchWorker("worker-2", "localhost", 9999);
        System.out.println("Workers started");

        Thread.sleep(5000);
        launcher.killAll();
    }
}
