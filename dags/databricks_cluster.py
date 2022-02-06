from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG('databricks_cluster', description='Databricks Test Dag',
          schedule_interval=None, 
          start_date=datetime(2022, 1, 20),
          catchup=False)

job_params = {
    "run_name":"sparkTask",
    "existing_cluster_id":"0125-085001-ilyvz75q",
    "notebook_task":{
        "notebook_path":"/Users/sunil0205@outlook.com/AirflowTest"
    }
}

databricks_job = DatabricksSubmitRunOperator(task_id="db_notebook_job",json=job_params,dag=dag)

databricks_job