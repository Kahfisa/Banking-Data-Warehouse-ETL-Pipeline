import os
from dotenv import load_dotenv
from project.extract.extract import extract_csv, extract_excel, extract_table
from project.transform.transform import transform_dimension_account, transform_dimension_branch, transform_dimension_customer, transform_fact_transaction
from project.load.load import load_bugquery

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_NAME = "DWH"

def main():
    df_transactioncsv = extract_csv()
    print("Data extracted from CSV:")
    print(df_transactioncsv.head())
    print(df_transactioncsv.info())

    df_transactionexcel = extract_excel()
    print("Data extracted from Excel:")
    print(df_transactionexcel.head())
    print(df_transactionexcel.info())

    df_account = extract_table("account")
    print("Data extracted from SQL Server table dbo.account:")
    print(df_account.head())
    print(df_account.info())

    df_branch = extract_table("branch")
    print("Data extracted from SQL Server table dbo.branch:")
    print(df_branch.head())
    print(df_branch.info())

    df_city = extract_table("city")
    print("Data extracted from SQL Server table dbo.city:")
    print(df_city.head())
    print(df_city.info())


    df_customer = extract_table("customer")
    print("Data extracted from SQL Server table dbo.customer:")
    print(df_customer.head())
    print(df_customer.info())


    df_state = extract_table("state")
    print("Data extracted from SQL Server table dbo.state:")
    print(df_state.head())
    print(df_state.info())

    df_transactiondb = extract_table("transaction_db")
    print("Data extracted from SQL Server table dbo.transaction_db:")
    print(df_transactiondb.head())
    print(df_transactiondb.info())    


    dftransformed_account = transform_dimension_account(df_account)
    print("Transformed Dimension Account:")
    print(dftransformed_account.head())
    print(dftransformed_account.info())

    dftransformed_branch = transform_dimension_branch(df_branch)
    print("Transformed Dimension Branch:")
    print(dftransformed_branch.head())
    print(dftransformed_branch.info())

    dftransformed_customer = transform_dimension_customer(df_customer, df_city, df_state)
    print("Transformed Dimension Customer:")
    print(dftransformed_customer.head())
    print(dftransformed_customer.info())

    dftransformed_transaction = transform_fact_transaction(df_transactioncsv, df_transactionexcel, df_transactiondb)
    print("Transformed Fact Transaction:")
    print(dftransformed_transaction.head())
    print(dftransformed_transaction.info())

    load_bugquery(dftransformed_account, "DIMAccount", DATASET_NAME, PROJECT_ID)
    print("Dataframe Account succesfully load to Bigquery")

    load_bugquery(dftransformed_branch, "DIMBranch", DATASET_NAME, PROJECT_ID)
    print("Dataframe Branch succesfully load to Bigquery")
    
    load_bugquery(dftransformed_customer, "DIMCustomer", DATASET_NAME, PROJECT_ID)
    print("Dataframe Customer succesfully load to Bigquery")

    load_bugquery(dftransformed_transaction, "FACTTransaction", DATASET_NAME, PROJECT_ID)
    print("Dataframe Transaction succesfully load to Bigquery")


if __name__ == "__main__":
    main()