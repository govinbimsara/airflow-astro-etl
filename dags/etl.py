from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.decorators.postgres.hooks.postgres import PostgresHooks
from airflow.utils.dates import days_ago
import json

with DAG(
    dag_id = 'nasa_apod_postgres',
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    #Create table to load data from api

    @task
    def create_table():
        #initialize postgreshook
        postgres_hook = PostgresHooks(postgres_conn_id = 'my_postgres_connection')

        #SQL to create table
        create_table_query = """
            CRREATE TABLE IF NOT EXISTS apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(225),
                explanation TEXT,
                url TEXT,
                data DATE,
                media_type VARCAHR(50)
            );
        
        """
        ##execute table creation query
        postgres_hook.run(create_table_query)

    ##Extract data from the NASA api
    extract_epod = SimpleHttpOperator(
        task_id = 'extract_apod',
        htttp_conn_id = 'nasa_api', #connection id that has been defined in airflow for NASA api
        endpoint = 'planetary/apod',
        method = 'GET',
        data = {"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"},
        response_filter = lambda response:response.json()
    )