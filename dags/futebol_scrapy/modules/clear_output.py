import os


def clean_all_outputs():
    if os.path.exists("dags/spain_table.csv"):
        os.remove("dags/spain_table.csv")

