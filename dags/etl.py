from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
        postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        #SQL to create table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(225),
                explanation TEXT,
                url TEXT,
                data DATE,
                media_type VARCHAR(50)
            );
        
        """
        ##execute table creation query
        postgres_hook.run(create_table_query)

    ##Extract data from the NASA api
    extract_epod = SimpleHttpOperator(
        task_id = 'extract_apod',
        http_conn_id = 'nasa_api', #connection id that has been defined in airflow for NASA api
        endpoint = 'planetary/apod',
        method = 'GET',
        data = {"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"},
        response_filter = lambda response:response.json()
    )

    ##Transform the APOD data
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }

        return apod_data

    ##Load data to Postgres SQL
    @task
    def data_load_to_postgres(apod_data):
        #Initiate postgress hook
        postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        #Sql insert query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        """

        postgres_hook.run(
            insert_query,
            parameters = (
                apod_data['title'],
                apod_data['explanation'],
                apod_data['url'],
                apod_data['date'],
                apod_data['media_type']
            )
        )

    ##Define task dependancies
    #Etract
    create_table() >> extract_epod
    api_response = extract_epod.output
    #Transform
    transformed_data = transform_apod_data(api_response)
    #Load
    data_load_to_postgres(transformed_data)