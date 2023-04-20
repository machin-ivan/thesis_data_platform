import datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from stg_load_script import run_stg_load_script
from stg_dds_migration import run_stg_dds_mirgation


DAG_ID = 'main_dag'

with DAG(
    dag_id=DAG_ID, 
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    load_staging = PythonOperator(task_id='load_staging',
                                  python_callable=run_stg_load_script)
    stg_dds_migration = PythonOperator(task_id='stg_dds_migration',
                                  python_callable=run_stg_dds_mirgation)
    

load_staging >> stg_dds_migration

