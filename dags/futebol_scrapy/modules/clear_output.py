import os
from airflow.decorators import task

@task()
def clean_all_outputs():
    if os.path.exists("dags/championship_table.csv"):
        os.remove("dags/championship_table.csv")

