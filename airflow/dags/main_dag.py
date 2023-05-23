import datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from stg_load_script import run_stg_load_script
from stg_dds_migration import run_stg_dds_mirgation
from rew_tokens_clusterize import rt_clusterize
from sql_helper_functions import clear_stg_func, reset_dds_func, main_datamart_create


DAG_ID = 'main_dag'

with DAG(
    dag_id=DAG_ID, 
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    dds_reset = PythonOperator(task_id='reset_dds', 
                               python_callable=reset_dds_func)
    load_staging = PythonOperator(task_id='load_staging',
                                  python_callable=run_stg_load_script)
    stg_dds_migration = PythonOperator(task_id='stg_dds_migration',
                                  python_callable=run_stg_dds_mirgation)
    clear_stg = PythonOperator(task_id='clear_staging', 
                               python_callable=clear_stg_func)
    rew_tokens_clusterize = PythonOperator(task_id='rew_tokens_clusterize', 
                               python_callable=rt_clusterize)
    create_main_datamart = PythonOperator(task_id='create_main_datamart', 
                               python_callable=main_datamart_create)
    
    

dds_reset >> load_staging >> stg_dds_migration >> clear_stg >> rew_tokens_clusterize >> create_main_datamart

