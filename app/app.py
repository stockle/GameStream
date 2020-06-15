import numpy
import pandas as pd
from pyspark.sql.functions import col
from database import connector, spark_connector
from flask import Flask, Markup, render_template, request

app = Flask(__name__)
app.debug = True

db = connector.DBConnector()
db.init_session()
db.init_keyspace('v1')

sdb = spark_connector.SparkConnector()

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

def spark_submit_query(form_data):
    users = load_and_get_table_df("v1", "users")
    gevents = load_and_get_table_df("v1", "gameplay_events")
    pevents = load_and_get_table_df("v1", "purchase_events")

    df = gevents.join(pevents,
        (pevents.platform == form_data['system_pc']
        | pevents.platform == form_data['system_ps4'])
    ).join(users,
        user.id == gevents.user_id
        & form_data['age_bracket_from'] <= user.age
        & user.age <= form_data['age_bracket_to']
    ).where(
        form_data['datetime_from']
        <= gevents.event_time
        & gevents.event_time
        <= form_data['datetime_to']
        & col('game').like(form_data['game_name'])
    )

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
    labels, values, system_stats = spark_submit_query(form_data)
    return render_template('index.html', title='PC Users per 100ms', max=max(values) + 1, labels=labels, values=values)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)