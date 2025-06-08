import io
from airflow.operators.python import PythonOperator
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from datetime import  datetime

from helpers.Layers import Layers
from helpers.config import POSTGRES_URI, AZURE_CONNECTION_STRING, CONTAINER_NAME
from helpers.database import delete_day_transactions,create_conn


engine = create_engine(POSTGRES_URI)
create_conn()

def transform_transactions(**kwargs):
    execution_date= kwargs["ds"]
    schema=Layers.BRONZE.value
    table="transactions"
    qualified_table = f"{schema}.{table}"
    query = f"""
        SELECT *
        FROM {qualified_table}
        WHERE date = '{execution_date}'
        """
    client_df= pd.read_sql_table("clients", con=engine, schema=Layers.SILVER.value)
    day_transactions = pd.read_sql(query, con=engine)
    columns_to_check = ['transaction_id','client_id', 'date','hour','minute','product_id','quantity','store_id']
    filtered_df = day_transactions.dropna(subset=columns_to_check).drop_duplicates(subset='transaction_id', keep='last')
    joined_df=pd.merge(filtered_df, client_df[['id', 'account_id']], left_on='client_id',right_on='id',how='inner').drop(columns=['id'])
    delete_day_transactions(execution_date, Layers.SILVER.value)
    joined_df.to_sql(table, engine, if_exists="append", index=False,schema=Layers.SILVER.value)


def load_transactions_raw(**kwargs):
    client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = client.get_container_client(CONTAINER_NAME)
    execution_date= kwargs["ds"]
    blobs = container.list_blobs(name_starts_with=f"transactions_{execution_date}")

    for blob in blobs:
        stream =container.get_blob_client(blob.name).download_blob(max_concurrency=1, encoding='UTF-8').readall()
        df = pd.read_csv(io.StringIO(stream), sep=';', comment='#') #ignore the comments in the csv
        table_name = "transactions"
        delete_day_transactions(execution_date, Layers.BRONZE.value)
        df.to_sql(table_name, engine, if_exists="append", index=False,schema=Layers.BRONZE.value)


with DAG(
        dag_id='ingestion_transactions_daily',
        schedule='0 9 * * *',  # Run daily at 9:00 AM
        start_date=datetime(2023, 11, 23),
        end_date=datetime(2023, 12, 1),
        catchup=True, # True to catch up the already received data
) as dag:

    wait_for_blob = WasbPrefixSensor(
        task_id='wait_for_transactions_blob',
        container_name=CONTAINER_NAME,
        prefix='transactions_{{ ds }}_',# prefix with yesterday’s date and underscore
        wasb_conn_id='azure_blob_conn',
        timeout=60 * 60 * 2,       # Wait up to 2 hours
        poke_interval=300,         # Check every 5 minutes
    )

    load_transactions_raw_operator = PythonOperator(
        task_id="load_transactions_raw",
        python_callable=load_transactions_raw,
        provide_context=True,
    )

    transform_transactions_operator = PythonOperator(
        task_id="transform_transactions",
        python_callable=transform_transactions,
        provide_context=True,
    )

    wait_for_blob >> load_transactions_raw_operator >> transform_transactions_operator

