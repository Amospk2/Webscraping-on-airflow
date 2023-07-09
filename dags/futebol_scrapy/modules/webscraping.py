from bs4 import BeautifulSoup
import pandas as pd
import requests
from airflow.decorators import task

def build_soup():
    web = 'https://www.gazetaesportiva.com/campeonatos/brasileiro-serie-a/'
    request = requests.get(web)
    content = request.text
    return BeautifulSoup(content, 'lxml')


def get_stats_from_table():
    soup = build_soup()
    table_times = soup.find(class_="table")

    position = []
    time_name = []
    p = []
    j = []
    v = []
    e = []
    d = []
    gp = []
    gc = []
    sg = []
    porc = []


    for times in table_times.find_all('tr')[1:]:
        position.append(times.find(class_="table__position").get_text().strip(" "))
        time_name.append(times.find(class_="table__team").get_text().strip(" "))

        stats = times.find_all(class_="table__stats")
        p.append(stats[0].get_text())
        j.append(stats[1].get_text())
        v.append(stats[2].get_text())
        e.append(stats[3].get_text())
        d.append(stats[4].get_text())
        gp.append(stats[5].get_text())
        gc.append(stats[6].get_text())
        sg.append(stats[7].get_text())
        porc.append(stats[8].get_text())


    return  {
        "id":position,
        "time":time_name,
        "p":p,
        "j":j,
        "v":v,
        "e":e,
        "d":d,
        "gp":gp,
        "gc":gc,
        "sg":sg,
        "porc":porc
    }

@task()
def start_pipeline():
    df = pd.DataFrame(get_stats_from_table())
    df.to_csv("dags/championship_table.csv", index=False)