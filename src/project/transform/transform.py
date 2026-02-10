import pandas as pd

def transform_dimension_account(df_account: pd.DataFrame) -> pd.DataFrame:
    """Transform dimension account data."""
    return pd.DataFrame({

        "AccountID": df_account["account_id"],
        "CustomerID": df_account["customer_id"],
        "AccountType": df_account["account_type"].str.title(),
        "Balance": df_account["balance"],
        "DateOpened": pd.to_datetime(df_account["date_opened"]),
        "Status": df_account["status"].str.title()
    })

def transform_dimension_branch(df_branch: pd.DataFrame) -> pd.DataFrame:
    """Transform dimension branch data."""
    return pd.DataFrame({

        "BranchID": df_branch["branch_id"],
        "BranchName": df_branch["branch_name"],
        "branch_location": df_branch["branch_location"]
    })

def transform_dimension_customer(df_customer: pd.DataFrame, df_city: pd.DataFrame, df_state: pd.DataFrame) -> pd.DataFrame:
    """Transform dimension customer data."""
    df = df_customer.merge(df_city, on="city_id", how="left")
    df = df.merge(df_state, on="state_id", how="left")

    df_result = pd.DataFrame({
        "CustomerID": df["customer_id"],
        "CityID": df["city_id"],
        "StateID": df["state_id"],
        "CustomerName": df["customer_name"],
        "Address": df["address"],
        "CityName": df["city_name"],
        "StateName": df["state_name"],
        "Age": df["age"],
        "Gender": df["gender"].str.title(),
        "Email": df["email"]
    })

    return df_result


def transform_fact_transaction(df_transactiondb: pd.DataFrame, df_transactionexcel: pd.DataFrame, df_transactioncsv: pd.DataFrame) -> pd.DataFrame:
    """Transform fact transaction data."""
    df = pd.concat([df_transactiondb, df_transactionexcel, df_transactioncsv], ignore_index=True)
    df = df.drop_duplicates(subset=["transaction_id"])

    df_result = pd.DataFrame({
        "TransactionID": df["transaction_id"],
        "AccountID": df["account_id"],
        "TransactionDate": pd.to_datetime(df["transaction_date"]),
        "Amount": df["amount"],
        "TransactionType": df["transaction_type"],
        "BranchID": df["branch_id"]
    })

    return df_result
