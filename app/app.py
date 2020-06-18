import os
import numpy
import pandas as pd
from datetime import datetime
from database import cassandra_connector
from pyspark.sql.functions import col, asc
from flask import Flask, Markup, render_template, request

db = cassandra_connector.DBConnector("v1")

app = Flask(__name__)
app.debug = True

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

def construct_user_query(form):
    where = ''
    
    if 'age_bracket_from' in form or 'age_bracket_to' in form:
        where += 'WHERE '
        if 'age_bracket_from' in form:
            users += f"""min_age > {form['age_bracket_from']} """
            if 'age_bracket_to' in form:
                query += f""" AND max_age < {form['age_bracket_to']} """
        elif 'age_bracket_to' in form:
            query += f"""max_age < {form['age_bracket_to']} """
    
    return where

def where_system(form):
    where = ''
    
    if 'system_pc' in form and 'system_ps4' in form:
        where = f"""WHERE platform = {form['system_pc']}
                    OR platform = {form['system_ps4']}
                """
    elif 'system_pc' in form:
        where = f"WHERE platform = {form['system_pc']}"
    elif 'system_ps4' in form:
        where = f"WHERE platform = {form['system_ps4']}"
    
    return where

def where_daterange(form):
    where = ''

    if 'datetime_from' in form or 'datetime_to' in form:
        where += ' WHERE '
        if 'datetime_from' in form:
            where += f" event_time > {datetime(form['datetime_from'])}"
        if 'datetime_to' in form:
            if where != ' WHERE ':
                where += ' AND ' 
            where += f" event_time < {datetime(form['datetime_to'])}"
    if 'game_name' in form:
        if where != '':
            where += ' AND ' 
        where += f"game LIKE {form['game_name']}"

    return where

def construct_query(form):
    users = 'SELECT * FROM users'
    users += construct_user_query(form)

    gevents = 'SELECT * FROM gameplay_events'
    pevents = 'SELECT * FROM purchase_events'

    where = where_system(form)
    gevents += where
    pevents += where

    where = where_daterange(form)
    gevents += where
    pevents += where

    return (gevents, pevents, users)

def submit_query(queries):
    users = db.select("users")
    gevents = db.select("gameplay_events")
    pevents = db.select("purchase_events")

    gevents = gevents.groupby(pd.Grouper(key='event_time', freq='1s')).event_time.agg('count').to_frame('count').reset_index()
    pevents = gevents.groupby(pd.Grouper(key='event_time', freq='1s')).event_time.agg('count').to_frame('count').reset_index()

    return {
        'users': users,
        'gameplay_events': gevents,
        'purchase_events': pevents
    }

@app.route('/')
def home():
    print(db)
    return render_template('index.html')

@app.route('/data', methods=["GET"])
def handle_form_submit():
    form_data = request.form
    
    queries = construct_query(form_data)
    data = submit_query(queries)
    
    return render_template(
        'form.html',
        title='PC Users per 100ms',
        max=max(values) + 1,
        labels=labels,
        gameplay_values=data['gameplay_values'],
        purchase_values=data['purchase_values'],
        user_demos=data['user_demographics']
    )

if __name__ == '__main__':
    app.run(port=8080)
