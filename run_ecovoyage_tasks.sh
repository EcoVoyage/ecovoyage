#!/bin/bash

# Set up environment variables
export AIRFLOW_HOME=/workspace/airflow
export TZ=UTC
export PYTHONPATH=/workspace:$PYTHONPATH

# List of tasks to run
TASKS=(
  "check_environment"
  "prepare_env"
  "download_data"
  "check_status"
  "run_tests"
  "check_test_results"
  "run_ecovoyage"
)

# Run each task in sequence
for task in "${TASKS[@]}"; do
  echo "==== Running task: $task ===="
  airflow tasks test ecovoyage_dag "$task" 2025-05-20
  
  # Check if the task succeeded
  if [ $? -ne 0 ]; then
    echo "Task $task failed!"
    exit 1
  fi
  
  echo "==== Task $task completed successfully ===="
  echo
done

echo "All tasks completed successfully!" 