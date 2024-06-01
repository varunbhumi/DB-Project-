#importing libraries 

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3
import csv
from io import StringIO

# Function to fetch political data from NY Times Archive using API Key and upload to S3 as CSV file
def fetch_data_and_upload_S3(**kwargs):
    # API key for accessing NY Times API
    api_key = 'DgzYa5BCtBTebGfflzYJOORk9ZhVxTMU'
    
    # Start date for fetching data (February 1, 2024) 
    start_date = datetime(2024, 2, 1)
    
    # End date is set to the current UTC date at midnight
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Initializing current_date to start_date
    current_date = start_date

    # Looping over dates from start_date to end_date
    while current_date <= end_date:
        # Extracting year and month from current_date
        year = current_date.year
        month = current_date.month
        
        # Constructing API URL for fetching data for the current month
        url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"
        
        # Send request to NY Times API
        response = requests.get(url)

        # Checking if request was successful (HTTP status code 200)
        if response.status_code == 200:
            # Parse JSON response
            data = response.json()
            
            # Extracting only politics-related articles
            politics_articles = [
                {key: article.get(key, '') for key in ['abstract', 'web_url', 'snippet', 'lead_paragraph', 'print_section', 'print_page', 'source', 'multimedia', 'headline', 'keywords', 'pub_date', 'document_type', 'news_desk', 'section_name', 'byline', 'type_of_material', '_id', 'word_count', 'uri', 'subsection_name']}
                for article in data['response']['docs']
                if 'Politics' in article.get('subsection_name', '')
            ]

            # Check if there are politics-related articles
            if politics_articles:
                # Initializing the Amazon S3 client
                s3 = boto3.client('s3')
                
                # Specifying S3 bucket name
                bucket_name = 'group-project-nyt'
                
                # Converting data to CSV format
                csv_buffer = StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=politics_articles[0].keys())
                writer.writeheader()
                writer.writerows(politics_articles)
                
                # Creating an object key for storing the file in S3
                object_key = f'NYTData/{year}/{month:02}.csv'
                
                # Upload the CSV data to S3
                s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())
                
                # Print success message
                print(f"Political data for {year}-{month:02} uploaded successfully to S3 as CSV")
        else:
            # Raise an exception if fetching data failed to log errors
            raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

        # Move to the next month
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fetch_political_nytimes_data_S3',
    default_args=default_args,
    description='Fetch Political data from NY Times Archive and upload to S3 as CSV',
    schedule_interval=timedelta(days=1),  # Running the DAG daily
    catchup=False  # Don't process old data when DAG is first run
) as dag:
    # Define a PythonOperator to execute the fetch_data_and_upload_S3 function(alotted task id)
    fetch_data_and_upload_task = PythonOperator(
        task_id='fetch_data_and_upload_S3',
        python_callable=fetch_data_and_upload_S3,
    )
