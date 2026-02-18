import re
from pathlib import Path

def strip_comments(content):
    # Remove single line comments
    content = re.sub(r'//.*', '', content)
    # Remove multi-line comments
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    return content

class ProtocolStructureTest:
    def __init__(self):
        pass
    
    def test_message_format(self):
        """Verify message format compliance with CSM218 protocol"""
        try:
            message_class_exists = Path("src/main/java/pdc/Message.java").exists()
            
            if not message_class_exists:
                return False, "Message class not found"
            
            with open("src/main/java/pdc/Message.java", "r") as f:
                content = strip_comments(f.read())
                
                required_fields = ["magic", "version", "messageType", "studentId", "timestamp", "payload"]
                missing_fields = []
                
                for field in required_fields:
                    if field not in content:
                        missing_fields.append(field)
                
                if missing_fields:
                    return False, f"Missing fields: {', '.join(missing_fields)}"
                
                if "CSM218" not in content:
                    return False, "CSM218 magic not found"
                
                return True, "Message format compliant"
        
        except Exception as e:
            return False, str(e)
    
    def test_serialization(self):
        """Verify messages are serialized properly"""
        try:
            serialization_found = False
            
            files = ["src/main/java/pdc/Message.java", "src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = strip_comments(f.read())
                        
                        # Check for serialization methods
                        if "toJson" in content or "toXml" in content or "serialize" in content.lower():
                            serialization_found = True
                        if "DataOutputStream" in content or "ObjectOutputStream" in content:
                            serialization_found = True
                except:
                    pass
            
            if serialization_found:
                return True, "Serialization logic found"
            else:
                return False, "No serialization logic detected"
        
        except Exception as e:
            return False, str(e)
    
    def test_protocol_validation(self):
        """Verify protocol validation"""
        try:
            validation_found = False
            
            files = ["src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = strip_comments(f.read())
                        
                        if "validate" in content.lower() or "parse" in content:
                            validation_found = True
                        if "exception" in content.lower() or "throw" in content.lower():
                            validation_found = True
                except:
                    pass
            
            if validation_found:
                return True, "Protocol validation found"
            else:
                return False, "No protocol validation"
        
        except Exception as e:
            return False, str(e)
    
    def test_no_external_frameworks(self):
        """Verify no external RPC/communication frameworks used"""
        try:
            forbidden_frameworks = ["grpc", "akka", "netty", "rmi", "jws", "soap", "corba"]
            found_frameworks = []
            
            java_files = list(Path("src").rglob("*.java"))
            
            for java_file in java_files:
                try:
                    with open(java_file, "r") as f:
                        content = f.read().lower()
                        
                        for framework in forbidden_frameworks:
                            if framework in content:
                                found_frameworks.append(framework)
                except:
                    pass
            
            if found_frameworks:
                return False, f"Forbidden frameworks found: {', '.join(set(found_frameworks))}"
            else:
                return True, "No external frameworks"
        
        except Exception as e:
            return False, str(e)
    
    def test_environment_variables(self):
        """Verify use of environment variables for configuration"""
        try:
            env_usage = False
            
            files = ["src/main/java/pdc/Master.java", "src/main/java/pdc/Worker.java"]
            
            for file_path in files:
                try:
                    with open(file_path, "r") as f:
                        content = strip_comments(f.read())
                        
                        if "getenv" in content or "System.getenv" in content:
                            env_usage = True
                        if "STUDENT_ID" in content or "PORT" in content:
                            env_usage = True
                except:
                    pass
            
            if env_usage:
                return True, "Environment variables used"
            else:
                return False, "No environment variable usage"
        
        except Exception as e:
            return False, str(e)
    
    def run_all(self):
        """Run all protocol structure tests"""
        results = {}
        
        success, msg = self.test_message_format()
        results["message_format"] = {"passed": success, "message": msg, "weight": 0.05}
        
        success, msg = self.test_serialization()
        results["serialization"] = {"passed": success, "message": msg, "weight": 0.00}
        
        success, msg = self.test_protocol_validation()
        results["protocol_validation"] = {"passed": success, "message": msg, "weight": 0.00}
        
        success, msg = self.test_no_external_frameworks()
        results["no_external_frameworks"] = {"passed": success, "message": msg, "weight": 0.10}
        
        success, msg = self.test_environment_variables()
        results["environment_variables"] = {"passed": success, "message": msg, "weight": 0.05}
        
        return results

if __name__ == "__main__":
    tester = ProtocolStructureTest()
    results = tester.run_all()
    
    for test_name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        print(f"{test_name}: {status} - {result['message']}")
