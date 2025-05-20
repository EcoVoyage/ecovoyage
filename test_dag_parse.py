import os
import sys

# Set up the environment
os.environ['AIRFLOW_HOME'] = '/workspace/airflow'
os.environ['TZ'] = 'UTC'

# Add the Airflow dags folder to sys.path
sys.path.append('/workspace/airflow/dags')

try:
    # First check if we can import the Airflow DAG class
    from airflow.models.dag import DAG
    print("Successfully imported DAG from airflow.models.dag")
    
    # Then try to import our DAG
    from ecovoyage_dag import dag
    print("Successfully imported dag from ecovoyage_dag")
    
    # Check if the DAG object is valid
    print(f"DAG ID: {dag.dag_id}")
    print(f"DAG Description: {dag.description}")
    print(f"DAG Tasks: {[task.task_id for task in dag.tasks]}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc() 