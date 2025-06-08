from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.settings import Session
import psycopg2
import pandas as pd
import io

from helpers.config import AZURE_CONNECTION_STRING

def delete_day_transactions(execution_date,schema):

    """
    Delete all transactions from the specified schema's transactions table for a given date.
    The purpose of deleting the data before inserting it is to ensure an idempotent behaviour ( for example
    even if we rerun the DAG we won't have duplicates inserted each time )

    Parameters:
    -----------
    execution_date : str
        The date (as a string) to filter transactions for deletion, expected in 'YYYY-MM-DD' format.
    schema : str
        The database schema containing the transactions table.
    """
    conn = psycopg2.connect(
        database="airflow", user='airflow',
        password='airflow', host='postgres', port='5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    sql = f"DELETE FROM {schema}.transactions where date ='{execution_date}'; "

    cursor.execute(sql)
    conn.commit()
    conn.close()


def create_conn():
    """
   Creates an Airflow connection for Azure Blob Storage if it does not already exist.
   """
    session = Session()
    conn_id = "azure_blob_conn"

    # Check if connection exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
    if existing_conn is None:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="wasb",
            extra={"connection_string": AZURE_CONNECTION_STRING }
        )
        session.add(new_conn)
        session.commit()


def blob_storage_to_postgres(blob,container,table_name,schema,engine):
    """
    Downloads a CSV file from Azure Blob Storage and appends its contents to a PostgreSQL table.

    Parameters
    ----------
    blob : A blob object representing the file to be downloaded.
    container :The container client used to access the blob storage.
    table_name : The name of the target PostgreSQL table to insert the data into.
    schema : The name of the schema in which the table exists.
    engine : SQLAlchemy engine connected to the PostgreSQL database.
    """

    stream =container.get_blob_client(blob.name).download_blob(max_concurrency=1, encoding='UTF-8').readall()
    df = pd.read_csv(io.StringIO(stream), sep=';', comment='#')
    df.to_sql(table_name, engine, if_exists="append", index=False, schema=schema)

