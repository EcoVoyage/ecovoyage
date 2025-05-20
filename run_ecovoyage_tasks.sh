#!/bin/bash

# Setup environment variables
export AIRFLOW_HOME=/workspace/airflow
export AIRFLOW_CONFIG=/workspace/airflow/airflow.cfg
export PYTHONPATH=/workspace:$PYTHONPATH
export TZ=UTC
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__LOGGING__LOGGING_LEVEL=CRITICAL

# Simple check environment
echo "Running check environment..."
echo "Working directory: $(pwd)"
echo "Python: $(which python)"
echo "Python version: $(python --version)"

# Run the main ecovoyage application
echo "Running ecovoyage application..."
cd /workspace && python -m ecovoyage.main

echo "All tasks completed successfully!" 