import os
import numpy
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, asc
from database import connector, spark_connector
from flask import Flask, Markup, render_template, request

app = Flask(__name__)
app.debug = True

# os.environ['PYSPARK_SUBMIT_ARGS'] = f"""
#         --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0
#         --conf spark.cassandra.connection.host={os.environ['DB_ADDR']}
#         --conf spark.cassandra.auth.username={os.environ['DB_USER']}
#         --conf spark.cassandra.auth.password={os.environ['DB_PASS']}
#         pyspark-shell
    # """

sdb = spark_connector.SparkConnector()

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

def join_df_tables(gevents, pevents, users, data):
    df = gevents.join(pevents).join(users, users.id == gevents.user_id)

    # join dates
    if 'datetime_from' in data:
        df = gevents.where(df.event_time > datetime(data['datetime_from']))
    if 'datetime_to' in data:
        df = gevents.where(df.event_time < datetime(data['datetime_to']))
    if 'game_name' in data:
        df = gevents.where(col('game').like(data['game_name']))

    # join system
    if 'system_pc' in data and 'system_ps4' in data:
        df.where(df.platform == data['system_pc'] | df.platform == data['system_ps4'])
    elif 'system_ps4' in data:
        df.where(df.platform == data['system_ps4'])
    elif 'system_pc' in data:
        df.where(df.platform == data['system_pc'])

    if 'age_bracket_from' in data:
        df.where(df.age > data['age_bracket_from'])
    if 'age_bracket_to' in data:
        df.where(df.age < data['age_bracket_to'])

    return query

def spark_submit_query(data):
    users = sdb.load_and_get_table_df("v1", "users")
    # user.registerTempTable("users")
    gevents = sdb.load_and_get_table_df("v1", "gameplay_events")
    # user.registerTempTable("gameplay_events")
    pevents = sdb.load_and_get_table_df("v1", "purchase_events")
    # user.registerTempTable("purchase_events")

    df = join_df_tables(gevets, pevents, users, data).orderBy(['event_time'], ascending=True)

    # df = gevents.join(pevents,
    #     (pevents.platform == form_data['system_pc']
    #     | pevents.platform == form_data['system_ps4'])
    # ).join(users,
    #     user.id == gevents.user_id
    #     & form_data['age_bracket_from'] <= user.age
    #     & user.age <= form_data['age_bracket_to']
    # ).where(
    #     form_data['datetime_from']
    #     <= gevents.event_time
    #     & gevents.event_time
    #     <= form_data['datetime_to']
    #     & col('game').like(form_data['game_name'])
    # )

    df['event_time'].astype('datetime64')
    lines = df.groupby(pd.Grouper(key='event_time', freq='100ms')).event_time.agg('count').to_frame('count').reset_index()
    df.show()
    values = lines['count'].values
    labels = lines['event_time'].values
    print(labels)

    return labels, values, {}

@app.route('/', methods=["GET", "POST"])
def handle_form_submit():
    form_data = request.form
    # app.logger.info('form submitted:', form_data)
    
    # query = construct_query(form_data)
    labels, values, system_stats = spark_submit_query(form_data)
    
    return render_template(
        'index.html',
        title='PC Users per 100ms',
        max=max(values) + 1,
        labels=labels,
        values=values
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
