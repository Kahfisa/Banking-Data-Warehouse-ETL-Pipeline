import os
from dotenv import load_dotenv
from project.extract.extract import extract_csv, extract_excel, extract_table
from project.transform.transform import transform_dimension_account, transform_dimension_branch, transform_dimension_customer, transform_fact_transaction
from project.load.load import load_bigquery

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_NAME = "DWH"

def main():
    """Extract Process: Extract data from CSV, Excel, and SQL Server tables."""
    try:
        df_transactioncsv = extract_csv()
        print("Data extracted from CSV:")
        print(df_transactioncsv.head())
        print(df_transactioncsv.info())
    except Exception as e:
        print(f"Error extracting CSV data: {e}")

    try:
        df_transactionexcel = extract_excel()
        print("Data extracted from Excel:")
        print(df_transactionexcel.head())
        print(df_transactionexcel.info())
    except Exception as e:
        print(f"Error extracting Excel data: {e}")

    tables = ["account", "branch", "city", "customer", "state", "transaction_db"]
    dataframes = {}

    for table in tables:
        try:
            df = extract_table(table)
            dataframes[table] = df
            print(f"Data extracted from SQL Server table dbo.{table}:")
            print(df.head())
            print(df.info())
        except Exception as e:
            print(f"Error extracting {table} data: {e}")

    """Transform Process: Transform data into dimension and fact tables."""
    transformed_data = {}

    for table in tables:
        try:
            if table == "account":
                df = transform_dimension_account(dataframes["account"])
                transformed_data["DIMAccount"] = df
            elif table == "branch":
                df = transform_dimension_branch(dataframes["branch"])
                transformed_data["DIMBranch"] = df
            elif table == "customer":
                df = transform_dimension_customer(dataframes["customer"], dataframes["city"], dataframes["state"])
                transformed_data["DIMCustomer"] = df
            elif table == "transaction_db":
                df = transform_fact_transaction(df_transactioncsv,df_transactionexcel,dataframes["transaction_db"])
                transformed_data["FACTTransaction"] = df
            else:
                continue 

            print(f"\nTransformed {table} successfully")
            print(df.head())
            print(df.info())

        except Exception as e:
            print(f"Error transforming {table}: {e}")

    """Load Process: Load transformed data into BigQuery."""
    for table_name, df in transformed_data.items():
        try:
            load_bigquery(df, table_name, DATASET_NAME, PROJECT_ID)
            print(f"{table_name} successfully loaded to BigQuery")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")

if __name__ == "__main__":
    main()
