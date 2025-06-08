from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine

from helpers.Layers import Layers
from helpers.config import AZURE_CONNECTION_STRING, CONTAINER_NAME, POSTGRES_URI
from helpers.database import blob_storage_to_postgres


engine = create_engine(POSTGRES_URI)
schema=Layers.BRONZE.value
target_tables = {
    "clients",
    "stores",
    "products"
}


def transform_referential():
    for table_name in target_tables:
        src_df= pd.read_sql_table(table_name, con=engine, schema=Layers.BRONZE.value)
        columns_to_check = ['id']
        filtered_df = src_df.dropna(subset=columns_to_check).drop_duplicates(subset='id', keep='last') #drop duplicates on pkey
        filtered_df.to_sql(table_name, engine, if_exists="append", index=False,schema=Layers.SILVER.value)


def ingestion_referential_init():
    client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = client.get_container_client(CONTAINER_NAME)
    blobs = container.list_blobs()


    for blob in blobs:
        filename = os.path.basename(blob.name)
        table_name = filename.removesuffix(".csv")
        if table_name in target_tables:
            blob_storage_to_postgres(blob,container,table_name,Layers.BRONZE.value,engine)


with DAG("ingestion_referential_init", schedule="@once", catchup=False, start_date=datetime(2023, 11, 23),) as dag:
    read_files = PythonOperator(
        task_id="ingestion_referential_init",
        python_callable=ingestion_referential_init,
    )

    transform_referential_operator = PythonOperator(
        task_id="transform_referential",
        python_callable=transform_referential,
        provide_context=True,
    )

    read_files>>transform_referential_operator