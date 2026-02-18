#!/bin/bash

set -e

echo "=== CSM218 Autograder Start ==="

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# ALWAYS run from the repository root to ensure all relative paths in tests work
cd "$REPO_ROOT"

# Robust python detection
if command -v python3 &>/dev/null; then
  PYTHON_EXE=python3
elif command -v python &>/dev/null; then
  PYTHON_EXE=python
else
  echo "Error: Python not found"
  exit 1
fi

# Run Python grading script with forwarded arguments
$PYTHON_EXE "$SCRIPT_DIR/grade.py" "$@"

echo "=== Autograder Complete ==="
