import re
from pathlib import Path

def strip_comments(content):
    # Remove single line comments
    content = re.sub(r'//.*', '', content)
    # Remove multi-line comments
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    return content

class ParallelExecutionTest:
    def __init__(self):
        self.task_timings = {}
        self.task_results = {}
    
    def test_parallel_matrix_multiply(self):
        """Verify matrix computation speedup suggests parallel execution"""
        # This is a static placeholder for now as the student has TODOs
        # In a real run, we would launch Master/Worker and measure timing
        # For now, we search for parallel patterns
        try:
            parallel_logic = False
            files = ["src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = strip_comments(f.read())
                        if "submit(" in content or "invokeAll" in content or "fork" in content:
                            parallel_logic = True
                except:
                    pass
            
            if parallel_logic:
                return True, "Parallel execution patterns detected"
            else:
                return False, "No parallel execution logic found"
        except Exception as e:
            return False, str(e)

    def check_concurrency_support(self):
        """Verify code supports concurrent requests"""
        try:
            concurrent_handling = False
            files = ["src/main/java/pdc/Master.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = strip_comments(f.read())
                        if "Thread" in content or "ExecutorService" in content:
                            concurrent_handling = True
                except:
                    pass
            
            if concurrent_handling:
                return True, "Concurrent execution support found"
            else:
                return False, "No concurrent execution support"
        except Exception as e:
            return False, str(e)
    
    def run_all(self):
        """Run all parallel execution tests"""
        results = {}
        
        success, msg = self.test_parallel_matrix_multiply()
        results["parallel_matrix_multiply"] = {"passed": success, "message": msg, "weight": 0.20}
        
        success, msg = self.check_concurrency_support()
        results["concurrency_support"] = {"passed": success, "message": msg, "weight": 0.00}
        
        return results

if __name__ == "__main__":
    tester = ParallelExecutionTest()
    results = tester.run_all()
    
    for test_name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        print(f"{test_name}: {status} - {result['message']}")
