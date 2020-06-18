import os
import numpy
import pandas as pd
from datetime import datetime
from database import spark_connector
from pyspark.sql.functions import col, asc
from flask import Flask, Markup, render_template, request

sdb = None

@app.before_first_request
def app_factory():
    sdb = spark_connector.SparkConnector()
    return Flask(__name__)
    
app = app_factory()
app.debug = True

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

def join_df_tables(gevents, pevents, users, data):
    users = sdb.load_and_get_table_df("v1", "users")
    gevents = sdb.load_and_get_table_df("v1", "gameplay_events")
    pevents = sdb.load_and_get_table_df("v1", "purchase_events")

    df = gevents.crossJoin(pevents)

    # join dates
    if 'datetime_from' in data:
        df = gevents.where(df.event_time > datetime(data['datetime_from']))
    if 'datetime_to' in data:
        df = gevents.where(df.event_time < datetime(data['datetime_to']))
    if 'game_name' in data:
        df = gevents.where(col('game').like(data['game_name']))

    # join system
    if 'system_pc' in data and 'system_ps4' in data:
        df.where(df.platform == 'PC' | df.platform == 'PS4')
    elif 'system_ps4' in data:
        df.where(df.platform == 'PC')
    elif 'system_pc' in data:
        df.where(df.platform == 'PS4')

    # if 'age_bracket_from' not in data:
    #   data['age_bracket_from'] = 13
    # if 'age_bracket_to' not in data:
    #   data['age_bracket_to'] = 75
    # df = df.join(users, (users.age > data['age_bracket_from']) & (users.age < data['age_bracket_to']) & (users.id == df.user_id))
    
    return df

def spark_submit_query(data):
    users = sdb.load_and_get_table_df("v1", "users")
    gevents = sdb.load_and_get_table_df("v1", "gameplay_events")
    pevents = sdb.load_and_get_table_df("v1", "purchase_events")

    df = join_df_tables(gevets, pevents, users, data).orderBy(['event_time'], ascending=True)

    df['event_time'].astype('datetime64')
    lines = df.groupby(pd.Grouper(key='event_time', freq='100ms')).event_time.agg('count').to_frame('count').reset_index()
    df.show()
    values = lines['count'].values
    labels = lines['event_time'].values
    print(labels)

    return labels, values, {}

@app.route('/')
def home():
    print(sdb)
    return render_template('index.html')

@app.route('/data', methods=["GET"])
def handle_form_submit():
    form_data = request.form
    # app.logger.info('form submitted:', form_data)
    
    # query = construct_query(form_data)
    labels, values, system_stats = spark_submit_query(form_data)
    
    return render_template(
        'data.html',
        title='PC Users per 100ms',
        max=max(values) + 1,
        labels=labels,
        values=values
    )



if __name__ == '__main__':
    app.run(port=8080)
