#!/usr/bin/env python3

import json
import sys
import os
import shutil
import subprocess
from pathlib import Path

import argparse

# Ensure output handles UTF-8 (especially on Windows)
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        # Fallback for older python versions
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# Import test modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tests'))

from test_rpc_basic import AutograderTest
from test_parallel_execution import ParallelExecutionTest
from test_failure_handling import FailureHandlingTest
from test_protocol_structure import ProtocolStructureTest
from test_concurrency import ConcurrencyTest
from test_advanced_protocol import AdvancedProtocolTest

# Hidden Test Detection (Only runs if injected during CI)
HIDDEN_TEST_AVAILABLE = False
try:
    from test_hidden_robustness import HiddenRobustnessTest
    HIDDEN_TEST_AVAILABLE = True
except ImportError:
    pass

class Grader:
    def __init__(self):
        self.results = {}
        self.total_score = 0.0
        
        # Determine repository root and environment
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.abspath(os.path.join(script_dir, os.pardir))

        if os.path.exists("/submission"):
            # GitHub Classroom environment: submission mounted at /submission
            self.submission_dir = "/submission"
            self.output_dir = "/autograder/results"
        else:
            # Regular GitHub Actions or local runs: use repository root
            self.submission_dir = repo_root
            self.output_dir = os.path.join(repo_root, "autograder", "results")

        self.output_path = os.path.join(self.output_dir, "results.json")
        
    def compile_code(self):
        """Compile student and reference code"""
        repo_root = self.submission_dir
        gradle_log_path = os.path.join(self.output_dir, 'gradle_build.log')
        os.makedirs(self.output_dir, exist_ok=True)

        # Choose wrapper based on platform and availability
        is_windows = sys.platform.startswith('win')
        wrapper_sh = os.path.join(repo_root, 'gradlew')
        wrapper_bat = os.path.join(repo_root, 'gradlew.bat')

        cmd = None
        shell = False
        if is_windows and os.path.exists(wrapper_bat):
            cmd = [wrapper_bat, 'build']
            shell = True
        elif (not is_windows) and os.path.exists(wrapper_sh):
            cmd = [wrapper_sh, 'build']
            shell = False
        elif shutil.which('gradle'):
            cmd = ['gradle', 'build']
            shell = False
        else:
            # nothing to run
            with open(gradle_log_path, 'w', encoding='utf-8') as f:
                f.write('No gradle wrapper or gradle executable found in PATH.')
            print('COMPILATION FAILED: no gradle wrapper or gradle found')
            return False

        try:
            proc = subprocess.run(cmd, cwd=repo_root, capture_output=True, timeout=300, text=True, shell=shell)
            # write log always for visibility
            with open(gradle_log_path, 'w', encoding='utf-8') as f:
                f.write(proc.stdout or '')
                f.write('\n')
                f.write(proc.stderr or '')

            if proc.returncode != 0:
                print('COMPILATION FAILED')
                print(proc.stderr)
                return False

            print('[OK] Compilation successful')
            return True
        except Exception as e:
            # write exception to log
            with open(gradle_log_path, 'w', encoding='utf-8') as f:
                f.write(str(e))
            print(f'[FAIL] Compilation error: {e}')
            return False
    
    def run_tests(self, filter_suite=None, filter_type=None):
        """Run all or specific test suites"""
        test_suites = [
            ("RPC", AutograderTest()),
            ("Parallel", ParallelExecutionTest()),
            ("Failure", FailureHandlingTest()),
            ("Protocol", ProtocolStructureTest()),
            ("Concurrency", ConcurrencyTest()),
            ("Advanced", AdvancedProtocolTest()),
        ]
        
        if HIDDEN_TEST_AVAILABLE:
            test_suites.append(("SystemConsistency", HiddenRobustnessTest()))
        
        if filter_suite:
            test_suites = [s for s in test_suites if s[0] == filter_suite]
        
        all_results = {}
        all_weights = {}
        
        for suite_name, tester in test_suites:
            print(f"\n--- {suite_name} ---")
            results = tester.run_all()
            
            for test_name, test_result in results.items():
                is_static = test_result.get("type") == "static" or "compilation" in test_name or "schema" in test_name or "framework" in test_name or "variable" in test_name or "support" in test_name or "collection" in test_name or "format" in test_name or "serialization" in test_name
                
                if filter_type == "static" and not is_static:
                    continue
                if filter_type == "dynamic" and is_static:
                    continue
                
                full_name = f"{suite_name}::{test_name}"
                p_status = "PASS" if test_result["passed"] else "FAIL"
                print(f"[{p_status}] {test_name}: {test_result['message']}")
                
                # Output Tag for GitHub Classroom points matching
                if test_result["passed"]:
                    tag_name = test_name.upper().replace(" ", "_")
                    print(f"[CLASSROOM_TAG] {tag_name} PASSED")
                
                all_results[full_name] = test_result["passed"]
                all_weights[full_name] = test_result.get("weight", 0)
        
        return all_results, all_weights
    
    def calculate_score(self, results, weights):
        """Calculate weighted score"""
        total_weight = sum(weights.values())
        weighted_score = 0.0
        
        for test_name, passed in results.items():
            weight = weights.get(test_name, 0)
            if passed:
                weighted_score += weight
        
        # Normalize to 100
        score = (weighted_score / total_weight * 100) if total_weight > 0 else 0
        return score
    
    def run(self, filter_suite=None, filter_type=None):
        """Execute full autograding pipeline"""
        # Change to repo root to ensure relative paths in tests work correctly
        os.chdir(self.submission_dir)
        
        print(f"=== CSM218 Autograder {'['+filter_suite+']' if filter_suite else ''} {'('+filter_type+')' if filter_type else ''} ===\n")
        
        # Compile code
        if not self.compile_code():
            score = 0.0
            results = {
                "score": score,
                "status": "FAILED",
                "message": "Compilation failed"
            }
            self.output_results(results)
            sys.exit(1)
        
        # Run tests
        try:
            test_results, test_weights = self.run_tests(filter_suite, filter_type)
        except Exception as e:
            print(f"Error running tests: {e}")
            test_results = {}
            test_weights = {}
        
        # Calculate score
        final_score = self.calculate_score(test_results, test_weights)
        
        # Determine status
        status = "PASS" if final_score >= 60.0 else "FAIL"
        
        results = {
            "score": round(final_score, 2),
            "status": status,
            "test_results": test_results,
            "test_weights": test_weights,
            "message": f"Final Score: {final_score:.2f}%"
        }
        
        self.output_results(results)
        print(f"\nStatus: {status}")
        print(f"=== Score for this section: {final_score:.2f}% ===")
        
        if filter_suite or filter_type:
            # Let GitHub Classroom string matching determine the points
            sys.exit(0)
        else:
            if status == "FAIL":
                sys.exit(1)
    
    def output_results(self, results):
        """Output results as JSON"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        with open(self.output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"Results written to {self.output_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='CSM218 Autograder')
    parser.add_argument('--suite', type=str, help='Run a specific test suite (RPC, Parallel, Failure, Protocol, Concurrency, Advanced)')
    parser.add_argument('--type', type=str, choices=['static', 'dynamic'], help='Filter by test type')
    args = parser.parse_args()
    
    grader = Grader()
    grader.run(filter_suite=args.suite, filter_type=args.type)
