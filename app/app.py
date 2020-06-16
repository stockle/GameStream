import numpy
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col
from database import connector, spark_connector
from flask import Flask, Markup, render_template, request

app = Flask(__name__)
app.debug = True

# os.environ['PYSPARK_SUBMIT_ARGS'] = f"""
#         --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0
#         --conf
#             spark.cassandra.connection.host={os.environ['DB_ADDR']}
#         pyspark-shell
#     """

db = connector.DBConnector()
db.init_session()
db.init_keyspace('v1')

sdb = spark_connector.SparkConnector()

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

def construct_query(data):
    query = f"""
        SELECT * FROM v1.gameplay_events AS ge
        JOIN v1.purchase_events AS pe
    """
    if 'system_pc' in data:
        query += f"""ON pe.platform={data['system_pc']} """
    if 'system_ps4' in data:
        query += f""" AND pe.platform={data['system_ps4']} """
    if 'age_bracket_from' in data or 'age_bracket_to' in data:
        query += f""" JOIN v1.users AS u"""
        if 'age_bracket_from' in data:
            query += f""" ON u.age > {data['age_bracket_from']}"""
            if 'age_bracket_to' in data:
                query += f""" AND u.age < {data['age_bracket_to']}"""
        elif 'age_bracket_to' in data:
            query += f""" ON u.age < {data['age_bracket_to']}"""
    if 'datetime_from' in data or 'datetime_to' in data or 'game_name' in data:
        query += f""" WHERE"""
        if 'datetime_from' in data:
            query += f""" ge.event_time > {datetime(data['datetime_from'])}"""
        if 'datetime_to' in data:
            query += f""" AND ge.event_time < {datetime(data['datetime_to'])}"""
        if 'game_name' in data:
            query ++ f""" AND ge.game LIKE {data['game_name']}"""
    return query

def spark_submit_query(form_data):
    df = sdb.submit_sql(query)

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
    values = lines['count'].values
    labels = lines['event_time'].values
    print(labels)

    return labels, values, {}

@app.route('/', methods=["GET", "POST"])
def handle_form_submit():
    form_data = request.form
    app.logger.info('form submitted:', form_data)
    
    query = construct_query(form_data)
    labels, values, system_stats = spark_submit_query(query)
    
    return render_template('index.html', title='PC Users per 100ms', max=max(values) + 1, labels=labels, values=values)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)