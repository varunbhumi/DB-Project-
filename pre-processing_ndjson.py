# Importing Libraries
import json  
from airflow import DAG  
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago  
from airflow.providers.amazon.aws.hooks.s3 import S3Hook   

# Function to recursively clean data structures
def clean_json(data):
    """Recursively clean data structures."""
    if isinstance(data, dict):
        return {k: clean_json(v) for k, v in data.items() if v not in [None, 'NaN']}
    elif isinstance(data, list):
        return [clean_json(item) for item in data if item not in [None, 'NaN']]
    return data

# Function to correct the format of timestamps in data
def fix_timestamps(data):
    """Correct the format of timestamps in data."""
    if 'pub_date' in data:
        data['pub_date'] = data['pub_date'].replace('::', ':')
        if len(data['pub_date']) > 3 and data['pub_date'][-3] != ':':
            data['pub_date'] = data['pub_date'][:-2] + ':' + data['pub_date'][-2:]
    return data

# Function to remove specified fields from the data
def remove_fields(data, fields_to_remove=['multimedia']):
    """Remove specified fields from the data."""
    for field in fields_to_remove:
        data.pop(field, None)
    return data

# Function to preprocess NDJSON data
def preprocess_ndjson(bucket, key, processed_key):
    """Load, clean, and save NDJSON data."""
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Instantiate S3Hook with the AWS connection ID
    data_string = s3_hook.read_key(key, bucket)  # Read NDJSON data from S3
    data = (json.loads(line) for line in data_string.splitlines())  # Parse NDJSON data
    processed_data = (json.dumps(remove_fields(fix_timestamps(clean_json(item)))) for item in data)  # Preprocess data
    processed_data_string = "\n".join(processed_data)  # Join processed data into a string
    s3_hook.load_string(processed_data_string, processed_key, bucket, replace=True)  # Save processed data back to S3

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  
    'start_date': days_ago(1), 
    'email_on_failure': False,  
    'email_on_retry': False,  
    'retries': 0,  
    'retry_delay': 300, 
}

# Define the DAG
with DAG(
    'ndjson_preprocessing',  
    default_args=default_args,  
    schedule_interval=timedelta(days=1),
    catchup=False  # Whether to catch up missed runs
) as dag:
    preprocess_task = PythonOperator(  
        task_id='preprocess_ndjson',  
        python_callable=preprocess_ndjson,  
        op_kwargs={  
            'bucket': 'group-project-nyt',  # S3 bucket name
            'key': 'FinalBigQ.ndjson',  # Key for the original NDJSON data
            'processed_key': 'RTQuery.ndjson'  # Key for the processed NDJSON data
        },
    )
