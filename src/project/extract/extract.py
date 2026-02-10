import pandas as pd
from pathlib import Path
from project.clients.sqlserver import get_sqlserver_connection

def extract_csv():
   """Extract data from CSV file and return as DataFrame."""
   dftransaction_csv = pd.read_csv(f"{Path(__file__).resolve().parents[3]}/data/raw/transaction_csv.csv")
   return dftransaction_csv

def extract_excel():
   """Extract data from Excel file and return as DataFrame."""
   dftransaction_excel = pd.read_excel(f"{Path(__file__).resolve().parents[3]}/data/raw/transaction_excel.xlsx")
   return dftransaction_excel

def extract_table(table_name: str, schema: str = "dbo") -> pd.DataFrame:
   """Extract data from SQL Server table and return as DataFrame."""
   engine = get_sqlserver_connection()
   query = f"SELECT * FROM {schema}.[{table_name}]"
   return pd.read_sql(query, engine)
