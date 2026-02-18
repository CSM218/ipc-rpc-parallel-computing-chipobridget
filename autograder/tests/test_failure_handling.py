import signal
import re
from pathlib import Path

def strip_comments(content):
    # Remove single line comments
    content = re.sub(r'//.*', '', content)
    # Remove multi-line comments
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    return content

class FailureHandlingTest:
    def __init__(self):
        self.processes = []
    
    def test_failure_detection(self):
        """Verify system detects worker failure via heartbeat"""
        try:
            heartbeat_logic = False
            timeout_logic = False
            
            files_to_check = ["src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]
            
            for file_path in files_to_check:
                try:
                    with open(file_path, "r") as f:
                        content = strip_comments(f.read())
                        if "heartbeat" in content.lower() or "ping" in content.lower() or "health" in content.lower():
                            heartbeat_logic = True
                        if "timeout" in content.lower():
                            timeout_logic = True
                except:
                    pass
            
            if heartbeat_logic and timeout_logic:
                return True, "Heartbeat and timeout logic detected"
            else:
                return False, "Missing heartbeat or timeout logic"
        
        except Exception as e:
            return False, str(e)
    
    def test_recovery_mechanism(self):
        """Verify system can recover from partial failures"""
        try:
            recovery_logic = False
            reassignment_logic = False
            
            with open("src/main/java/pdc/Master.java", "r") as f:
                content = strip_comments(f.read())
                if "retry" in content.lower() or "recover" in content.lower():
                    recovery_logic = True
                if "reassign" in content.lower() or "redistribute" in content.lower():
                    reassignment_logic = True
            
            if recovery_logic or reassignment_logic:
                return True, "Recovery mechanism found"
            else:
                return False, "No recovery mechanism"
        
        except Exception as e:
            return False, str(e)
    
    def run_all(self):
        """Run all failure handling tests"""
        results = {}
        
        success, msg = self.test_failure_detection()
        results["worker_failure_detection"] = {"passed": success, "message": msg, "weight": 0.05}
        
        success, msg = self.test_recovery_mechanism()
        results["recovery_mechanism"] = {"passed": success, "message": msg, "weight": 0.10}
        
        return results

if __name__ == "__main__":
    tester = FailureHandlingTest()
    results = tester.run_all()
    
    for test_name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        print(f"{test_name}: {status} - {result['message']}")
