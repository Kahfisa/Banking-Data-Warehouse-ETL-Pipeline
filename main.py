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

    try:
        df_account = extract_table("account")
        print("Data extracted from SQL Server table dbo.account:")
        print(df_account.head())
        print(df_account.info())
    except Exception as e:
        print(f"Error extracting account data: {e}")

    try:
        df_branch = extract_table("branch")
        print("Data extracted from SQL Server table dbo.branch:")
        print(df_branch.head())
        print(df_branch.info())
    except Exception as e:
        print(f"Error extracting branch data: {e}")

    try:
        df_city = extract_table("city")
        print("Data extracted from SQL Server table dbo.city:")
        print(df_city.head())
        print(df_city.info())
    except Exception as e:
        print(f"Error extracting city data: {e}")

    try:
        df_customer = extract_table("customer")
        print("Data extracted from SQL Server table dbo.customer:")
        print(df_customer.head())
        print(df_customer.info())
    except Exception as e:
        print(f"Error extracting customer data: {e}")

    try:
        df_state = extract_table("state")
        print("Data extracted from SQL Server table dbo.state:")
        print(df_state.head())
        print(df_state.info())
    except Exception as e:
        print(f"Error extracting state data: {e}")

    try:
        df_transactiondb = extract_table("transaction_db")
        print("Data extracted from SQL Server table dbo.transaction_db:")
        print(df_transactiondb.head())
        print(df_transactiondb.info())    
    except Exception as e:
        print(f"Error extracting transaction_db data: {e}")

    """Transform Process: Transform data into dimension and fact tables."""
    try:
        dftransformed_account = transform_dimension_account(df_account)
        print("Transformed Dimension Account:")
        print(dftransformed_account.head())
        print(dftransformed_account.info())
    except Exception as e:
        print(f"Error transforming account data: {e}")

    try:
        dftransformed_branch = transform_dimension_branch(df_branch)
        print("Transformed Dimension Branch:")
        print(dftransformed_branch.head())
        print(dftransformed_branch.info())
    except Exception as e:
        print(f"Error transforming branch data: {e}")

    try:
        dftransformed_customer = transform_dimension_customer(df_customer, df_city, df_state)
        print("Transformed Dimension Customer:")
        print(dftransformed_customer.head())
        print(dftransformed_customer.info())
    except Exception as e:
        print(f"Error transforming customer data: {e}")

    try:
        dftransformed_transaction = transform_fact_transaction(df_transactioncsv, df_transactionexcel, df_transactiondb)
        print("Transformed Fact Transaction:")
        print(dftransformed_transaction.head())
        print(dftransformed_transaction.info())
    except Exception as e:
        print(f"Error transforming transaction data: {e}")

    """Load Process: Load transformed data into BigQuery."""
    try:
        load_bigquery(dftransformed_account, "DIMAccount", DATASET_NAME, PROJECT_ID)
        print("Dataframe Account succesfully load to Bigquery")
    except Exception as e:
        print(f"Error loading account data to BigQuery: {e}")

    try:
        load_bigquery(dftransformed_branch, "DIMBranch", DATASET_NAME, PROJECT_ID)
        print("Dataframe Branch succesfully load to Bigquery")
    except Exception as e:
        print(f"Error loading branch data to BigQuery: {e}")

    try:
        load_bigquery(dftransformed_customer, "DIMCustomer", DATASET_NAME, PROJECT_ID)
        print("Dataframe Customer succesfully load to Bigquery")
    except Exception as e:
        print(f"Error loading customer data to BigQuery: {e}")

    try:
        load_bigquery(dftransformed_transaction, "FACTTransaction", DATASET_NAME, PROJECT_ID)
        print("Dataframe Transaction succesfully load to Bigquery")
    except Exception as e:
        print(f"Error loading transaction data to BigQuery: {e}")


if __name__ == "__main__":
    main()