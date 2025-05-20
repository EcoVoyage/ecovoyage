from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
import os

# Add the project root to the path so we can import ecovoyage
sys.path.insert(0, '/workspace')

default_args = {
    'owner': 'ecovoyage',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecovoyage_dag',
    default_args=default_args,
    description='EcoVoyage processing workflow',
    schedule='@daily',  # Updated for Airflow 3.0
    start_date=datetime(2025, 5, 20),
    catchup=False,
    tags=['ecovoyage'],
) as dag:

    # Task to check environment
    env_check = BashOperator(
        task_id='check_environment',
        bash_command='echo "Running in $(pwd)" && echo "Python: $(which python)" && echo "Python version: $(python --version)"',
    )

    # Task to prepare environment for running tests
    prepare_env = BashOperator(
        task_id='prepare_env',
        bash_command='mkdir -p /workspace/airflow/test_reports',
    )

    # Task to run ecovoyage download (if available)
    download_data = BashOperator(
        task_id='download_data',
        bash_command='cd /workspace && pixi run download || echo "Download task not available"',
    )

    # Define a Python function to be used in a PythonOperator
    def check_ecovoyage_status():
        try:
            # Check if src/ecovoyage exists
            ecovoyage_path = '/workspace/src/ecovoyage'
            if os.path.exists(ecovoyage_path):
                print(f"EcoVoyage module directory exists: {ecovoyage_path}")
                print(f"Contents: {os.listdir(ecovoyage_path)}")
            else:
                print(f"EcoVoyage module directory does not exist: {ecovoyage_path}")
                
            # Check if we can read some basic info
            if os.path.exists(os.path.join(ecovoyage_path, '__init__.py')):
                with open(os.path.join(ecovoyage_path, '__init__.py'), 'r') as f:
                    print(f"EcoVoyage __init__.py content: {f.read()}")
        except Exception as e:
            print(f"Error checking EcoVoyage files: {e}")
        
        # Check if data directory exists
        data_path = '/workspace/data'
        if os.path.exists(data_path):
            print(f"Data directory exists: {data_path}")
            print(f"Contents: {os.listdir(data_path)}")
        else:
            print(f"Data directory does not exist: {data_path}")
        
        return "EcoVoyage status check completed"

    # Task to check ecovoyage status
    check_status = PythonOperator(
        task_id='check_status',
        python_callable=check_ecovoyage_status,
    )
    
    # Task to run all tests directly (without depending on installation)
    run_tests = BashOperator(
        task_id='run_tests',
        bash_command='''
            cd /workspace && 
            export PYTHONPATH=/workspace && 
            python -m unittest discover -s src/ecovoyage/tests > /workspace/airflow/test_reports/test_results.txt 2>&1 || 
            echo "Tests completed with errors, see test_reports/test_results.txt for details"
        ''',
    )
    
    # Task to run the main ecovoyage application using pixi
    run_ecovoyage = BashOperator(
        task_id='run_ecovoyage',
        bash_command='cd /workspace && pixi run run || echo "Run task not available"',
    )
    
    # Task to check test results
    check_test_results = BashOperator(
        task_id='check_test_results',
        bash_command='cat /workspace/airflow/test_reports/test_results.txt',
    )
    
    # Set up the task dependencies
    env_check >> prepare_env >> download_data >> check_status >> run_tests >> check_test_results >> run_ecovoyage 