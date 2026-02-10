import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pyodbc


load_dotenv()

def get_sqlserver_connection():
    """Initialize SQL Server connection using environment variables for configuration."""
    username = os.getenv("USERNAME")
    driver = os.getenv("DRIVER")
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")

    connection_string = (
        f"UID={username};"
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}")
    return engine

    # conn = pyodbc.connect(connection_string) 
    # return conn
