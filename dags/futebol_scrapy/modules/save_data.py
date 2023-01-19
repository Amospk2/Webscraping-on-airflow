import os

def format_all_teams():
    teams = []
    if os.path.exists("dags/spain_table.csv"):
        with open('dags/spain_table.csv') as f:
            for lines in f.readlines()[1:]:
                line = lines.split(',')
                line[-1] = line[-1].replace('\n', '')
                for count, item in enumerate(line):
                    if count != 1:
                        line[count] = int(item)
                teams.append(line)
    
    return teams

def build_insert_or_update_query(team):
    return f"""
    INSERT INTO time (id, time, p, j, v, e, d, gp, gc, sg, porc)
    VALUES ({team[0]}, '{team[1]}', {team[2]}, {team[3]}, {team[4]}, {team[5]}, 
    {team[6]}, {team[7]}, {team[8]}, {team[9]}, {team[10]})
    ON CONFLICT (id) DO UPDATE 
        SET time = '{team[1]}', 
            p = {team[2]}, 
            j = {team[3]}, 
            v = {team[4]}, 
            e = {team[5]}, 
            d = {team[6]}, 
            gp = {team[7]}, 
            gc = {team[8]},
            sg = {team[9]}, 
            porc = {team[10]}; """

def save_data_in_database():
    teams =  format_all_teams()
    insert_or_update_query = """"""
    for time in teams:
        insert_or_update_query += build_insert_or_update_query(time)
    return insert_or_update_query
