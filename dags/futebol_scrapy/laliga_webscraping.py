from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime

import sys
sys.path.append("/opt/airflow/dags/futebol_scrapy/")
from modules import clear_output, save_data, webscraping

default_args = {
    'owner':'Amospk2',
    'retry':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="LaLiga_webscraping_dag",
    start_date=datetime(2023, 1, 19),
    end_date=datetime(2023, 6, 4),
    schedule_interval="0 0 * * 2"
) as dag:

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

    run_pipeline = PythonOperator(
        task_id="run_pipeline",
        python_callable=webscraping.start_pipeline
    )

    save_data = PostgresOperator(
        task_id="save_data_in_database",
        postgres_conn_id="airflow_postgres",
        sql=save_data.save_data_in_database()
    )

    clean_outputs = PythonOperator(
        task_id="clean_all_outputs",
        python_callable=clear_output.clean_all_outputs
    )

    set_db_configs_if_dont_exists >> run_pipeline >> save_data >> clean_outputs
     
