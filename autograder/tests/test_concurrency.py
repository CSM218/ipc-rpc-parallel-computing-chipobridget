import json
from pathlib import Path

class ConcurrencyTest:
    def __init__(self):
        pass
    
    def test_thread_support(self):
        """Verify support for multiple concurrent threads"""
        try:
            thread_support = False
            
            files = ["src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        
                        if "Thread" in content or "Runnable" in content or "Callable" in content:
                            thread_support = True
                        if "Executor" in content or "ThreadPool" in content:
                            thread_support = True
                except:
                    pass
            
            if thread_support:
                return True, "Thread support found"
            else:
                return False, "No thread support"
        
        except Exception as e:
            return False, str(e)
    
    def test_concurrent_collections(self):
        """Verify use of thread-safe collections"""
        try:
            concurrent_collections = False
            
            files = ["src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        
                        if "ConcurrentHashMap" in content or "CopyOnWriteArrayList" in content:
                            concurrent_collections = True
                        if "synchronized" in content or "Lock" in content:
                            concurrent_collections = True
                except:
                    pass
            
            if concurrent_collections:
                return True, "Concurrent collections used"
            else:
                return False, "No concurrent collection support"
        
        except Exception as e:
            return False, str(e)
    
    def test_connection_handling(self):
        """Verify server accepts multiple connections"""
        try:
            multi_conn = False
            
            with open("src/main/java/pdc/Master.java", "r") as f:
                content = f.read()
                
                # Check for accept() in loop or thread pool
                if "accept()" in content and ("while" in content or "Thread" in content):
                    multi_conn = True
                if "ExecutorService" in content:
                    multi_conn = True
            
            if multi_conn:
                return True, "Multiple connection handling found"
            else:
                return False, "No multi-connection support"
        
        except Exception as e:
            return False, str(e)
    
    def test_atomic_operations(self):
        """Verify atomic operations or proper synchronization"""
        try:
            atomic_support = False
            
            files = ["src/main/java/pdc/Master.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        
                        if "Atomic" in content or "synchronized" in content:
                            atomic_support = True
                        if "volatile" in content:
                            atomic_support = True
                except:
                    pass
            
            if atomic_support:
                return True, "Atomic/synchronized access found"
            else:
                return False, "No atomic operations"
        
        except Exception as e:
            return False, str(e)
    
    def test_request_queuing(self):
        """Verify requests can be queued for processing"""
        try:
            queuing = False
            
            files = ["src/main/java/pdc/Master.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        
                        if "Queue" in content or "BlockingQueue" in content:
                            queuing = True
                        if "submit(" in content and "ExecutorService" in content:
                            queuing = True
                except:
                    pass
            
            if queuing:
                return True, "Request queuing mechanism found"
            else:
                return False, "No request queuing"
        
        except Exception as e:
            return False, str(e)
    
    def run_all(self):
        """Run all concurrency tests"""
        results = {}
        
        success, msg = self.test_thread_support()
        results["thread_support"] = {"passed": success, "message": msg, "weight": 0.05}
        
        success, msg = self.test_concurrent_collections()
        results["concurrent_collections"] = {"passed": success, "message": msg, "weight": 0.05}
        
        # Merge these into the above or keep as 0 if not tracked separately in Classroom
        success, msg = self.test_connection_handling()
        results["connection_handling"] = {"passed": success, "message": msg, "weight": 0.00}
        
        success, msg = self.test_atomic_operations()
        results["atomic_operations"] = {"passed": success, "message": msg, "weight": 0.00}
        
        success, msg = self.test_request_queuing()
        results["request_queuing"] = {"passed": success, "message": msg, "weight": 0.00}
        
        return results

if __name__ == "__main__":
    tester = ConcurrencyTest()
    results = tester.run_all()
    
    for test_name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        print(f"{test_name}: {status} - {result['message']}")
