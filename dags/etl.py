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
    pass