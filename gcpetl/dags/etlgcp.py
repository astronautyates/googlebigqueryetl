from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import requests
import json
import os
import pandas as pd
from google.cloud import bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
}

@dag(default_args=default_args, schedule_interval=None, catchup=False)
def extended_etl_pipeline():

    @task()
    def extract_data_from_multiple_apis(api_urls: list):
        results = []
        for url in api_urls:
            response = requests.get(url)
            if response.status_code == 200:
                results.append(response.json())
            else:
                send_slack_alert(f"API request failed for {url} with status code {response.status_code}")
                raise Exception(f"API request failed for {url}")
        
        # Combine results and save them locally
        local_file_path = '/tmp/api_combined_data.json'
        with open(local_file_path, 'w') as outfile:
            json.dump(results, outfile)

        return local_file_path
    
    @task()
    def load_data_to_gcs(local_file_path: str, bucket_name: str, object_name: str):
        gcs_hook = GCSHook()
        gcs_hook.upload(bucket_name, object_name, local_file_path)
        return f"gs://{bucket_name}/{object_name}"
    
    @task()
    def validate_data(gcs_uri: str):
        gcs_hook = GCSHook()
        bucket_name, object_name = gcs_uri.replace("gs://", "").split("/", 1)

        # Download data from GCS
        data = gcs_hook.download(bucket_name, object_name)
        api_data = json.loads(data.decode("utf-8"))

        # Example validation: check if the data has required fields
        if not all('entries' in entry for entry in api_data):
            send_slack_alert(f"Data validation failed for {gcs_uri}")
            raise ValueError(f"Data validation failed: missing required fields")

        return gcs_uri
    
    @task()
    def transform_and_aggregate_data(gcs_uri: str):
        gcs_hook = GCSHook()
        bucket_name, object_name = gcs_uri.replace("gs://", "").split("/", 1)
        
        # Download data from GCS
        data = gcs_hook.download(bucket_name, object_name)
        api_data = json.loads(data.decode("utf-8"))

        # Simple aggregation (count of APIs per category)
        df = pd.DataFrame(api_data["entries"])
        aggregated_data = df.groupby("Category").size().reset_index(name="API_Count")
        
        # Save aggregated data
        aggregated_file_path = '/tmp/aggregated_data.json'
        aggregated_data.to_json(aggregated_file_path, orient='records')

        return aggregated_file_path
    
    @task()
    def load_data_to_bigquery(aggregated_file_path: str, dataset_id: str, table_id: str):

        bq_hook = BigQueryHook()
        client = bigquery.Client()

        # Load transformed data from GCS
        bucket_name = "your-gcs-bucket"
        object_name = "transformed_data/aggregated_data.json"
        gcs_hook = GCSHook()
        gcs_hook.upload(bucket_name, object_name, aggregated_file_path)

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("Category", "STRING"),
                bigquery.SchemaField("API_Count", "INTEGER"),
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",  # Keep the existing data and append new records
        )

        uri = f"gs://{bucket_name}/{object_name}"
        load_job = client.load_table_from_uri(uri, f"{dataset_id}.{table_id}", job_config=job_config)
        load_job.result()  # Wait for the job to complete

        return f"Data loaded to BigQuery: {dataset_id}.{table_id}"
    
    @task()
    def send_slack_alert(message: str):
        slack_webhook = SlackWebhookHook(webhook_token='your-slack-webhook-token')
        slack_webhook.send(text=message)

    api_urls = [
        "https://api.publicapis.org/entries",
        "https://api.example.com/another-endpoint"
    ]

    local_file_path = extract_data_from_multiple_apis(api_urls)
    gcs_uri = load_data_to_gcs(local_file_path, "your-gcs-bucket", "raw_data/api_combined_data.json")
    validated_gcs_uri = validate_data(gcs_uri)
    aggregated_file_path = transform_and_aggregate_data(validated_gcs_uri)
    load_data_to_bigquery(aggregated_file_path, "your_dataset", "your_table")

# Instantiate the DAG
extended_etl_dag = extended_etl_pipeline()
