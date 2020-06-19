import os
import numpy
import pandas as pd
from datetime import datetime
from database import cassandra_connector
from pyspark.sql.functions import col, asc
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
    
    if 'age_bracket_from' in form or 'age_bracket_to' in form:
        where += ' WHERE '
        if 'age_bracket_from' in form:
            where += f"""min_age > {form['age_bracket_from']} """
            if 'age_bracket_to' in form:
                where += f""" AND max_age < {form['age_bracket_to']} """
        elif 'age_bracket_to' in form:
            where += f"""max_age < {form['age_bracket_to']} """
    
    return where

def where_system(form):
    where = ''
    
    if 'system_pc' in form and 'system_ps4' in form:
        where = f""" WHERE platform = 'PC'
                    OR platform = 'PS4'
                """
    elif 'system_pc' in form:
        where = f" WHERE platform = 'PC'"
    elif 'system_ps4' in form:
        where = f" WHERE platform = 'PS4'"

    return where

def where_daterange(form, where):

    if where != '':
        where += ' AND '
    else:
        where += ' WHERE '

    if form['datetime_from'] != '' or form['datetime_to'] != '':
        if form['datetime_from'] != '':
            print(form['datetime_from'])
            date = datetime.strptime(form['datetime_from'], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S.%f")
            where += f" event_time > '{date}'"
        if form['datetime_to'] != '':
            if where != ' WHERE ':
                where += ' AND '
            date = datetime.strptime(form['datetime_to'], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S.%f")
            where += f" event_time < '{date}'"
    if form['game_name'] != '':
        if where != ' WHERE ' and where != ' AND ':
            where += ' AND '
        where += f" game LIKE '{form['game_name']}'"

    return where

def construct_query(form):
    users = "SELECT * FROM users"

    gevents = 'SELECT * FROM gameplay_events'
    pevents = 'SELECT * FROM purchase_events'
    if not ''.join(forms.values()):
        users += ' LIMIT 1000'
        gevents += ' LIMIT 1000'
        pevents += ' LIMIT 1000'
    else:
        users += construct_user_query(form)

        where = where_system(form)
        where = where_daterange(form, where)

        gevents += where
        pevents += where

    return (users + ';', gevents + ';', pevents + ';')

def submit_query(queries):
    print(queries)

    users = pd.DataFrame(list(db.select(queries[0])))
    gevents = pd.DataFrame(list(db.select(queries[1])))
    pevents = pd.DataFrame(list(db.select(queries[2])))

    print(pevents)
    
    pevents = pevents.groupby(pd.Grouper(key='event_time', freq='60s')).event_time.agg('count').to_frame('count').reset_index()
    gevents = gevents.groupby(pd.Grouper(key='event_time', freq='60s')).event_time.agg('count').to_frame('count').reset_index()
    users = users.groupby('min_age', 'max_age').min_age.agg('count').to_frame('count').reset_index()
    
    print(values)
    print(users)

    values = pd.merge(gevents, pevents, on='event_time').sort_values(by='event_time')

    return {
        'user_demographics': users[~users['id'].isin(values)],
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

    return render_template(
        'data.html',
        title='Users per 10s',
        max=max(data['values']['count_x'].values) + 10,
        date_labels=data['values']['event_time'].values,
        gameplay_values=data['values']['count_x'].values,
        purchase_values=data['values']['count_y'].values,
        user_demos=data['user_demographics'].values
    )

if __name__ == '__main__':
    app.run(port=8080)
