import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def get_bigquery_client():
    """Intialize Bigquery client using environment variables for configuration."""
    project_id = os.getenv("GCP_PROJECT_ID")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    return bigquery.Client.from_service_account_json(credentials_path, project=project_id)

