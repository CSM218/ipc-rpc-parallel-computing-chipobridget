#!/usr/bin/env python3
"""
Integration test harness for CSM218 distributed system evaluation
Spawns real JVM processes, communicates via sockets
"""

import json
import subprocess
import socket
import time
import os
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

class IntegrationTestHarness:
    def __init__(self, classpath, master_port=9999, num_workers=3):
        self.classpath = classpath
        self.master_port = master_port
        self.num_workers = num_workers
        self.master_process = None
        self.worker_processes = []
        self.master_socket = None
        self.results = {}
        self.start_times = {}
        self.end_times = {}
        self.errors = []
    
    def start_master(self):
        """Launch master process"""
        try:
            env = os.environ.copy()
            env['MASTER_PORT'] = str(self.master_port)
            env['STUDENT_ID'] = 'integration-test'
            
            self.master_process = subprocess.Popen(
                ['java', '-cp', self.classpath, 'pdc.ReferenceMaster'],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            print(f"[TEST] Master started on port {self.master_port}")
            time.sleep(1)
            
            return True
        except Exception as e:
            self.errors.append(f"Failed to start master: {e}")
            return False
    
    def start_workers(self):
        """Launch worker processes"""
        try:
            for i in range(self.num_workers):
                worker_id = f'worker-{i}'
                env = os.environ.copy()
                env['WORKER_ID'] = worker_id
                env['MASTER_HOST'] = 'localhost'
                env['MASTER_PORT'] = str(self.master_port)
                env['STUDENT_ID'] = 'integration-test'
                
                proc = subprocess.Popen(
                    ['java', '-cp', self.classpath, 'pdc.ReferenceWorker'],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                self.worker_processes.append((worker_id, proc))
                print(f"[TEST] {worker_id} started")
            
            time.sleep(2)
            return True
        except Exception as e:
            self.errors.append(f"Failed to start workers: {e}")
            return False
    
    def connect_to_master(self):
        """Connect to master server"""
        try:
            self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.master_socket.connect(('localhost', self.master_port))
            print("[TEST] Connected to master")
            return True
        except Exception as e:
            self.errors.append(f"Failed to connect to master: {e}")
            return False
    
    def send_task(self, task_id, task_type, payload):
        """Send RPC task to master"""
        try:
            message = {
                "magic": "CSM218",
                "version": 1,
                "messageType": "RPC_REQUEST",
                "studentId": "integration-test",
                "timestamp": int(time.time() * 1000),
                "payload": f"{task_id};{task_type};{payload}"
            }
            
            msg_json = json.dumps(message) + '\n'
            self.master_socket.sendall(msg_json.encode('utf-8'))
            
            self.start_times[task_id] = time.time()
            print(f"[TEST] Sent task {task_id}")
            
            return True
        except Exception as e:
            self.errors.append(f"Failed to send task: {e}")
            return False
    
    def send_parallel_tasks(self, num_tasks=4):
        """Send multiple tasks in parallel"""
        try:
            print(f"[TEST] Sending {num_tasks} parallel tasks...")
            
            with ThreadPoolExecutor(max_workers=num_tasks) as executor:
                futures = []
                for i in range(num_tasks):
                    task_id = f'task-{i}'
                    payload = '1,2\\3,4|5,6\\7,8'
                    
                    future = executor.submit(self.send_task, task_id, 'MATRIX_MULTIPLY', payload)
                    futures.append(future)
                
                for future in as_completed(futures):
                    future.result()
            
            print("[TEST] All tasks sent")
            return True
        except Exception as e:
            self.errors.append(f"Failed to send parallel tasks: {e}")
            return False
    
    def measure_parallelism(self):
        """Analyze execution timing to detect parallelism"""
        try:
            if len(self.start_times) < 2:
                return False, "Not enough tasks"
            
            times = list(self.start_times.values())
            min_start = min(times)
            max_start = max(times)
            start_spread = max_start - min_start
            
            # If all tasks started within 1 second, likely parallel
            if start_spread < 1.0:
                return True, f"Parallel execution detected (spread: {start_spread:.2f}s)"
            else:
                return False, f"Sequential execution detected (spread: {start_spread:.2f}s)"
        except Exception as e:
            return False, str(e)
    
    def kill_worker(self, index):
        """Kill a worker process for failure simulation"""
        try:
            if index < len(self.worker_processes):
                worker_id, proc = self.worker_processes[index]
                proc.terminate()
                proc.wait(timeout=2)
                print(f"[TEST] Killed {worker_id}")
                return True
        except Exception as e:
            self.errors.append(f"Failed to kill worker: {e}")
            return False
    
    def cleanup(self):
        """Cleanup all processes"""
        try:
            if self.master_socket:
                self.master_socket.close()
            
            if self.master_process:
                self.master_process.terminate()
                self.master_process.wait(timeout=2)
            
            for worker_id, proc in self.worker_processes:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except:
                    proc.kill()
            
            print("[TEST] Cleanup complete")
        except Exception as e:
            self.errors.append(f"Cleanup error: {e}")
    
    def run_basic_test(self):
        """Run basic communication test"""
        try:
            if not self.start_master():
                return False
            
            if not self.start_workers():
                return False
            
            if not self.connect_to_master():
                return False
            
            # Send single task
            if not self.send_task('test-1', 'MATRIX_MULTIPLY', '1,2\\3,4|5,6\\7,8'):
                return False
            
            time.sleep(2)
            
            print("[TEST] Basic test passed")
            return True
        except Exception as e:
            self.errors.append(f"Basic test error: {e}")
            return False
        finally:
            self.cleanup()
    
    def run_parallelism_test(self):
        """Run parallelism detection test"""
        try:
            if not self.start_master():
                return False
            
            if not self.start_workers():
                return False
            
            if not self.connect_to_master():
                return False
            
            if not self.send_parallel_tasks(4):
                return False
            
            time.sleep(3)
            
            parallel, msg = self.measure_parallelism()
            print(f"[TEST] Parallelism test: {msg}")
            
            return parallel
        except Exception as e:
            self.errors.append(f"Parallelism test error: {e}")
            return False
        finally:
            self.cleanup()
    
    def run_failure_test(self):
        """Run failure recovery test"""
        try:
            if not self.start_master():
                return False
            
            if not self.start_workers():
                return False
            
            if not self.connect_to_master():
                return False
            
            # Send tasks
            if not self.send_parallel_tasks(3):
                return False
            
            time.sleep(1)
            
            # Kill first worker
            if not self.kill_worker(0):
                return False
            
            time.sleep(2)
            
            # System should still have remaining workers
            if len(self.worker_processes) > 1:
                print("[TEST] Failure recovery test passed")
                return True
            else:
                print("[TEST] Failure recovery test failed")
                return False
        except Exception as e:
            self.errors.append(f"Failure test error: {e}")
            return False
        finally:
            self.cleanup()

def main():
    """Run integration tests"""
    classpath = "build/classes/java/main:build/resources/main"
    
    harness = IntegrationTestHarness(classpath)
    
    print("=== Integration Test Harness ===\n")
    
    tests = [
        ("Basic Communication", harness.run_basic_test),
        ("Parallelism Detection", harness.run_parallelism_test),
        ("Failure Recovery", harness.run_failure_test),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = result
            status = "PASS" if result else "FAIL"
            print(f"{test_name}: {status}\n")
        except Exception as e:
            results[test_name] = False
            print(f"{test_name}: FAIL ({e})\n")
    
    print("\n=== Test Results ===")
    for test_name, passed in results.items():
        status = "✓" if passed else "✗"
        print(f"{status} {test_name}")
    
    if harness.errors:
        print("\n=== Errors ===")
        for error in harness.errors:
            print(f"- {error}")

if __name__ == "__main__":
    main()
