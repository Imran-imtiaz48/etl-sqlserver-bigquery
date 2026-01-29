import time
import pandas as pd
import pypyodbc as odbc
from sqlserver import SQLServer
from google.cloud import bigquery
from google.oauth2 import service_account


# BigQuery setup

KEY_PATH = r"C:\Users\Moham\Downloads\groovy-datum-269018-c20afc51d870.json"

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)

# SQL Server setup

SERVER_NAME = r'DESKTOP-OFHRKAE'
DATABASE_NAME = 'test'

sql_server_instance = SQLServer(SERVER_NAME, DATABASE_NAME)

try:
    sql_server_instance.connect_to_sql_server()
    print("Connected to SQL Server")
except Exception as e:
    print(f"Failed to connect to SQL Server: {e}")
    exit(1)

# ----------------------
# SQL Query
# ----------------------
sql_statement = """
SELECT *
FROM [test].[HumanResources].[Department]
"""

try:
    columns, records = sql_server_instance.query(sql_statement)
    if not records:
        print("No data returned from SQL Server.")
        exit(0)
except Exception as e:
    print(f"SQL query failed: {e}")
    exit(1)

# Create DataFrame
df = pd.DataFrame(records, columns=columns)


# Load to BigQuery


DATASET_ID = 'etl_python1'
TABLE_ID = 'department'
TARGET_TABLE = f"{client.project}.{DATASET_ID}.{TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

try:
    job = client.load_table_from_dataframe(df, TARGET_TABLE, job_config=job_config)
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
    print("Data loaded successfully.")
except Exception as e:
    print(f"Failed to load data to BigQuery: {e}")
    exit(1)


# Verify loaded table

try:
    table = client.get_table(TARGET_TABLE)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to '{TARGET_TABLE}'"
    )
except Exception as e:
    print(f"Failed to fetch table info: {e}")
