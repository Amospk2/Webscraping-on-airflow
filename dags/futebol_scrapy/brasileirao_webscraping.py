import json
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

from airflow.decorators import dag, task

import sys
sys.path.append("/opt/airflow/dags/futebol_scrapy/")
from modules import clear_output, save_data, webscraping


default_args = {
    'owner':'Amospk2',
    'retry':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    schedule=None,
    default_args=default_args,
    start_date=pendulum.datetime(2023, 7, 9, tz="UTC"),
    catchup=False,
    dag_id="brasileirao_webscraping_dag",
    schedule_interval="0 0 * * 2"
)
def tutorial_taskflow_api():

    set_db_configs_if_dont_exists = PostgresOperator(
        task_id='set_db_configs_if_dont_exists',
        postgres_conn_id="airflow_postgres",
        sql="""
            create table if not exists Time(
                id int NOT NULL PRIMARY KEY,
                time varchar(255) UNIQUE,
                p int,
                j int,
                v int,
                e int,
                d int,
                gp int,
                gc int,
                sg int,
                porc int
            )
        """
    )

    loading = PostgresOperator(
        task_id="save_data_in_database",
        postgres_conn_id="airflow_postgres",
        sql=save_data.save_data_in_db()
    )

    set_db_configs_if_dont_exists >> webscraping.start_pipeline() >> loading >> clear_output.clean_all_outputs()

tutorial_taskflow_api()
