# Importing libraries
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

# Defining the main DAG
with DAG(
    'main_dag',  # Name of the main DAG
    description='Main DAG to orchestrate sub DAGs',  # Description of the main DAG
    schedule_interval="@daily",  # Schedule interval set to daily
    start_date=datetime(2024, 1, 1),  # Start date of the DAG
    catchup=False,  # Whether or not to catch up missed DAG runs
    tags=['example']  # Tags associated with the DAG
) as dag:  # Create the DAG instance

    # Trigger operators for each sub-DAG

    # Trigger DAG to fetch data and upload to S3
    trigger_csv_dag = TriggerDagRunOperator(
        task_id='fetch_data_and_upload_S3',  # Task ID for triggering the DAG
        trigger_dag_id='fetch_political_nytimes_data_S3',  # ID of the DAG to trigger
    )

    # Trigger DAG for incremental load of CSV data to S3
    trigger_csv_incremental_load_dag = TriggerDagRunOperator(
        task_id='incremental_load_task',  # Task ID
        trigger_dag_id='incremental_load_csv_s3',  # ID of the DAG to trigger
    )

    # Trigger DAG to convert JSON to NDJSON format
    trigger_j_to_ndj_dag = TriggerDagRunOperator(
        task_id='convert_json_to_ndjson_task',  # Task ID
        trigger_dag_id='convert_json_to_ndjson',  # ID of the DAG to trigger
    )

    # Trigger DAG for preprocessing NDJSON data
    trigger_query_ready_dag = TriggerDagRunOperator(
        task_id='preprocess_ndjson',  # Task ID
        trigger_dag_id='ndjson_preprocessing',  # ID of the DAG to trigger
    )

    # Set dependencies between tasks
    trigger_csv_dag >> trigger_csv_incremental_load_dag >> trigger_j_to_ndj_dag >> trigger_query_ready_dag
