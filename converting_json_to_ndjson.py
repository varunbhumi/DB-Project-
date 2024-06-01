#importing libraries

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import json

#function to convert to ndjson
def convert_json_to_ndjson(input_bucket, input_key, output_bucket, output_key):
    s3 = boto3.client('s3')
    
    # Download JSON file from S3
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    json_content = json.load(obj['Body'])
    
    # Convert JSON to NDJSON
    ndjson_content = ""
    if isinstance(json_content, list):  # Assuming the top level of JSON is a list
        for entry in json_content:
            ndjson_content += json.dumps(entry) + "\n"
    else:
        ndjson_content = json.dumps(json_content) + "\n"
    
    # Upload converted NDJSON to S3
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=ndjson_content)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': ,
}

dag = DAG(
    'convert_json_to_ndjson',
    default_args=default_args,
    description='Convert JSON file to NDJSON and upload to S3',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

convert_task = PythonOperator(
    task_id='convert_json_to_ndjson_task',
    python_callable=convert_json_to_ndjson,
    op_kwargs={
        'input_bucket': 'group-project-nyt',
        'input_key': 'GCS/PreBigQ.json',
        'output_bucket': 'group-project-nyt',
        'output_key': 'FinalBigQ.ndjson',
    },
    dag=dag,
)