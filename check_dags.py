import os
os.environ['AIRFLOW_HOME'] = '/workspace/airflow'
os.environ['TZ'] = 'UTC'

from airflow.models import DagBag

dagbag = DagBag()
print('Available DAGs:', list(dagbag.dags.keys()))
print('Errors:', dagbag.import_errors) 