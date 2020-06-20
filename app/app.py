import os
import numpy
import pandas as pd
from datetime import datetime
from database import cassandra_connector
from flask import Flask, Markup, render_template, request

db = cassandra_connector.DBConnector()
db.init_session(keyspace='v1')

app = Flask(__name__)
app.debug = True

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

def construct_user_query(form):
    where = ''
    
    if form['age_bracket_from'] != '' or form['age_bracket_to'] != '':
        where += ' WHERE '
        if form['age_bracket_from'] != '':
            where += f"""min_age > {form['age_bracket_from']} """
            if form['age_bracket_to'] != '':
                where += f""" AND max_age < {form['age_bracket_to']} """
        elif form['age_bracket_to'] != '':
            where += f"""max_age < {form['age_bracket_to']} """
        else:
            where = ''

    return where

def where_system(form):
    where = ''

    if 'system' in form:
        if form['system'] == 'system_pc' and form['system'] == 'system_ps4':
            where = f""" WHERE platform IN ('PC', 'PS4')"""
        elif form['system'] == 'system_pc':
            where = f" WHERE platform = 'PC'"
        elif form['system'] == 'system_ps4':
            where = f" WHERE platform = 'PS4'"

    return where

def where_daterange(form, where):
    print(where)
    if where == '':
        where = ' WHERE '
    else:
        where += ' AND '
    print(where)

    if form['datetime_from'] != '' or form['datetime_to'] != '':
        if form['datetime_from'] != '':
            date = datetime.strptime(form['datetime_from'], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S.%f")
            where += f" event_time > '{date}'"
        if form['datetime_to'] != '':
            if where != ' WHERE ':
                where += ' AND '
            date = datetime.strptime(form['datetime_to'], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S.%f")
            where += f" event_time < '{date}'"
    if form['game_name'] != '':
        if where != ' WHERE ' and where != ' AND ' and where:
            where += ' AND '
        elif where == '':
            where = ' WHERE '
        where += f" game LIKE '{form['game_name']}'"

    return where

def construct_query(form):
    users = "SELECT * FROM users"

    gevents = 'SELECT * FROM gameplay_events'
    pevents = 'SELECT * FROM purchase_events'
    if not ''.join(form.values()):
        users += ' LIMIT 1000'
        gevents += ' LIMIT 1000'
        pevents += ' LIMIT 1000'
    else:
        users += construct_user_query(form)

        where = where_system(form)
        where = where_daterange(form, where)

        gevents += where
        pevents += where

    return (users + ' ALLOW FILTERING;', gevents + ' ALLOW FILTERING;', pevents + ' ALLOW FILTERING;')

def submit_query(queries):
    print(queries)

    users = pd.DataFrame(list(db.select(queries[0])))
    gevents = pd.DataFrame(list(db.select(queries[1])))
    pevents = pd.DataFrame(list(db.select(queries[2])))

    print(users, gevents, pevents)

    users = users[~users['id'].isin(pevents)]
    users = users[~users['id'].isin(gevents)]

    pevents['event_time'] = pevents['event_time'].astype('datetime64')
    gevents['event_time'] = gevents['event_time'].astype('datetime64')

    pevents['count'] = pevents.groupby(pd.Grouper(key='event_time', freq='1s'))['event_time'].transform('count')
    gevents['count'] = gevents.groupby(pd.Grouper(key='event_time', freq='1s'))['event_time'].transform('count')
    # print(pevents)
    # print(gevents)
    values = gevents.merge(pevents, on='event_time')
    # print(values)
    values = values.sort_values(by='event_time')
    users = pd.DataFrame(users.groupby(pd.cut(users.min_age, [13,20,30,40,50,60,75])).min_age.agg('count').to_frame('count')).reset_index().dropna()

    print(values)

    if values.empty:
        values = pd.DataFrame({'count_x':[0], 'count_y':[0], 'event_time':[0]})

    return {
        'user_demographics': users,
        'values': values
    }

@app.route('/')
def home():
    print(db)
    return render_template('index.html')

@app.route('/data', methods=["GET", "POST"])
def handle_form_submit():
    form_data = request.form

    print(form_data)

    queries = construct_query(form_data)
    data = submit_query(queries)
    #print(data)
    print(data['values']['count_y'].values)

    system = ''

    if 'system' in form_data:
        if form_data['system'] == 'system_pc' and form_data['system'] == 'system_ps4':
            system = "PC & PS4"
        elif form_data['system'] == 'system_pc':
            system = "PC"
        elif form_data['system'] == 'system_ps4':
            system = "PS4"

    return render_template(
        'data.html',
        title='Active Users | Active Purchases',
        max=max(data['values']['count_x'].values) + 10,
        date_labels=data['values']['event_time'].values,
        gameplay_values=data['values']['count_x'].values,
        purchase_values=data['values']['count_y'].values,
        user_counts=data['user_demographics']['count'].values,
        system=system,
        user_demos=['13-19', '20-29', '30-39', '40-49', '50-59', '60-75']
    )

if __name__ == '__main__':
    app.run(port=8080)
