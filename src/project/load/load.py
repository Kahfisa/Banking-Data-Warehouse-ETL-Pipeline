from google.cloud import bigquery
from project.clients.bigquery import get_bigquery_client
import pandas as pd

client = get_bigquery_client()

def load_bigquery(df: pd.DataFrame, table_name: str, dataset_name: str, project_id: str):
    """Load a DataFrame into a BigQuery table."""
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_dataframe(df, table_id, job_config = job_config)
    load_job.result()

    print(f"Loaded {len(df)} rows into {table_id}.")
    
