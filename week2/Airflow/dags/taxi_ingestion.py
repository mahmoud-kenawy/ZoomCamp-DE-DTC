from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import os


def load_csv_to_postgres(taxi_type, postgres_conn_id='postgres_default'):
  
    base_path = f"/usr/local/airflow/include/taxi/{taxi_type}"
    table_name = f"{taxi_type}_taxi_data"
    
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Directory {base_path} does not exist.")

    for file in os.listdir(base_path):
        if file.endswith('.csv'):
            file_path = os.path.join(base_path, file)
            
     
            for df in pd.read_csv(file_path, chunksize=100000):
               
                date_cols = [col for col in df.columns if 'datetime' in col]
                for col in date_cols:
                    df[col] = pd.to_datetime(df[col])
                
               
                df.to_sql(table_name, engine, if_exists='append', index=False)
            

with DAG(
    dag_id='taxi_data_ingestion_v1',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    setup_tables = PostgresOperator(
        task_id='setup_tables',
        postgres_conn_id='postgres_default',
        sql='sql/create_tables.sql'
    )


    load_yellow = PythonOperator(
        task_id='load_yellow_taxi',
        python_callable=load_csv_to_postgres,
        op_kwargs={'taxi_type': 'yellow'}
    )


    load_green = PythonOperator(
        task_id='load_green_taxi',
        python_callable=load_csv_to_postgres,
        op_kwargs={'taxi_type': 'green'}
    )

    setup_tables >> [load_yellow, load_green]
