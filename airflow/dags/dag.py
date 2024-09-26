from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime


default_args = {
    'owner': 'Guilherme Pessoni',
    'start_date': datetime(2024, 9, 26),
    'retries': 1,
}


with DAG(
    dag_id='Case_Tecnico_Nestle',
    default_args=default_args,
    schedule_interval= '0 8 * * *',  
    catchup=False,) as dag:


    landing_to_bronze = DatabricksSubmitRunOperator(
        task_id='file_to_bronze',
        databricks_conn_id='databricks_default',  
        existing_cluster_id = 'ID_DO_CLUSTER',
        notebook_task={
            'notebook_path': 'PATH_PARA_NOTEBOOK/Landing_to_Bronze.py', 
        },)

    bronze_to_silver = DatabricksSubmitRunOperator(
        task_id='bronze_to_silver',
        databricks_conn_id='databricks_default',  
        existing_cluster_id = 'ID_DO_CLUSTER',
        notebook_task={
            'notebook_path': 'PATH_PARA_NOTEBOOK/Bronze_to_Silver.py', 
        },)

    silver_to_gold = DatabricksSubmitRunOperator(
        task_id='silver_to_gold',
        databricks_conn_id='databricks_default',  
        existing_cluster_id = 'ID_DO_CLUSTER',
        notebook_task={
            'notebook_path': 'PATH_PARA_NOTEBOOK/Silver_to_Gold.py', 
        },)

    landing_to_bronze >> bronze_to_silver >> silver_to_gold