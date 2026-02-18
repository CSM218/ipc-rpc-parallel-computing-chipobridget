import json
import subprocess
import time
import socket
import threading
import os
from pathlib import Path

class AutograderTest:
    def __init__(self):
        self.results = {}
        self.port_counter = 10000
    
    def compile_student_code(self):
        """Verify student code was compiled by main grader"""
        try:
            # Check for build artifacts
            if os.path.exists("build/classes/java/main/pdc/Message.class"):
                return True, "Compilation verified"
            return False, "Build artifacts not found"
        except Exception as e:
            return False, str(e)
    
    def check_protocol_schema(self):
        """Verify message protocol contains required CSM218 fields"""
        try:
            test_code = '''
import json
msg = {
    "magic": "CSM218",
    "version": 1,
    "messageType": "TEST",
    "studentId": "test",
    "timestamp": 123456,
    "payload": ""
}
print(json.dumps(msg))
'''
            required_fields = ["magic", "version", "messageType", "studentId", "timestamp", "payload"]
            
            # Check if student code validates protocol
            has_magic_check = False
            has_version_check = False
            
            with open("src/main/java/pdc/Message.java", "r") as f:
                content = f.read()
                has_magic_check = "CSM218" in content and "magic" in content
                has_version_check = "version" in content
            
            if has_magic_check and has_version_check:
                return True, "Protocol schema validated"
            else:
                return False, "Missing protocol validation"
        except Exception as e:
            return False, str(e)
    
    def check_socket_communication(self):
        """Verify socket-based IPC is used"""
        try:
            has_socket = False
            has_rmi = False
            has_grpc = False
            
            files_to_check = [
                "src/main/java/pdc/Master.java",
                "src/main/java/pdc/Worker.java"
            ]
            
            for file_path in files_to_check:
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        if "Socket" in content or "ServerSocket" in content:
                            has_socket = True
                        if "java.rmi" in content:
                            has_rmi = True
                        if "io.grpc" in content or "com.google.protobuf" in content:
                            has_grpc = True
                except:
                    pass
            
            if has_rmi or has_grpc:
                return False, "Must use socket IPC, not RMI/gRPC"
            if has_socket:
                return True, "Socket communication implemented"
            else:
                return False, "No socket communication found"
        except Exception as e:
            return False, str(e)
    
    def check_rpc_abstraction(self):
        """Verify RPC abstraction layer"""
        try:
            rpc_found = False
            
            for file_path in ["src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]:
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        if "RPC" in content or "rpc" in content or "request" in content.lower():
                            rpc_found = True
                except:
                    pass
            
            if rpc_found:
                return True, "RPC abstraction found"
            else:
                return False, "RPC abstraction not found"
        except Exception as e:
            return False, str(e)
    
    def run_all(self):
        """Run all basic tests"""
        results = {}
        
        success, msg = self.compile_student_code()
        results["compilation"] = {"passed": success, "message": msg, "weight": 0.05}
        
        success, msg = self.check_socket_communication()
        results["socket_ipc"] = {"passed": success, "message": msg, "weight": 0.10}
        
        success, msg = self.check_rpc_abstraction()
        results["rpc_abstraction"] = {"passed": success, "message": msg, "weight": 0.10}
        
        success, msg = self.check_protocol_schema()
        results["protocol_schema"] = {"passed": success, "message": msg, "weight": 0.05}
        
        return results

if __name__ == "__main__":
    tester = AutograderTest()
    results = tester.run_all()
    
    for test_name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        print(f"{test_name}: {status} - {result['message']}")
