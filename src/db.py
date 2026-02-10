import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from project.clients.bigquery import get_bigquery_client

load_dotenv()

client = get_bigquery_client()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_NAME = "DWH"
DATASET_LOCATION = "asia-southeast2"

def create_dataset():
    """Create a bigquery dataset""" 
    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = DATASET_LOCATION

    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} successfully created.")

def create_tables_dimensionaccount():
    """Create table dimension account in DWH dataset"""
    sql_path = Path(__file__).resolve().parent/"project"/"sql"/"data_definition_language"/"dimension_account.sql"
    
    with open(sql_path, "r", encoding="utf-8") as file:
        query = file.read()

    query = query.format(project_id=PROJECT_ID, dataset=DATASET_NAME)

    execution_query = client.query(query)
    execution_query.result()
    print("Table DIMAccount successfully created.")

def create_table_dimensionbranch():
    """Create table dimension branch in DWH dataset"""
    sql_path = Path(__file__).resolve().parent/"project"/"sql"/"data_definition_language"/"dimension_branch.sql"
    
    with open(sql_path, "r", encoding="utf-8") as file:
        query = file.read()

    query = query.format(project_id=PROJECT_ID, dataset=DATASET_NAME)

    execution_query = client.query(query)
    execution_query.result()
    print("Table DIMBranch successfully created.")

def create_table_dimensioncustomer():
    """Create table dimension customer in DWH dataset"""
    sql_path = Path(__file__).resolve().parent/"project"/"sql"/"data_definition_language"/"dimension_customer.sql"

    with open(sql_path, "r", encoding="utf-8") as file:
        query = file.read()

    query = query.format(project_id=PROJECT_ID, dataset=DATASET_NAME)

    execution_query = client.query(query)
    execution_query.result()
    print("Table DIMCustomer successfully created.")

def create_table_facttransaction():
    """Create table fact transaction in DWH dataset"""
    sql_path = Path(__file__).resolve().parent/"project"/"sql"/"data_definition_language"/"fact_transaction.sql"
   
    with open(sql_path, "r", encoding="utf-8") as file:
        query = file.read()

    query = query.format(project_id=PROJECT_ID, dataset=DATASET_NAME)
    
    execution_query = client.query(query)
    execution_query.result()
    print("Table FACTTransaction successfully created.")

if __name__ == "__main__":
    create_dataset()
    create_tables_dimensionaccount()
    create_table_dimensionbranch()
    create_table_dimensioncustomer()
    create_table_facttransaction()

