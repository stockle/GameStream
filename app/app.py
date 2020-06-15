import numpy
import pandas as pd
from database import connector
from flask import Flask, Markup, render_template

app = Flask(__name__)

db = connector.DBConnector()
db.init_session()
db.init_keyspace('v1')

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

def construct_query(data):
    query = """
        SELECT * FROM gameplay_events
        WHERE platform = 'PC'
        ALLOW FILTERING;
    """
    return query

def spark_submit_query(query):
    values = db.select()
    line_values = []
    df = pd.DataFrame(list(values))
    df['event_time'].astype('datetime64')
    lines = df.groupby(pd.Grouper(key='event_time', freq='100ms')).event_time.agg('count').to_frame('count').reset_index()
    values = lines['count'].values
    labels = lines['event_time'].values

    print(labels)

    return labels, values

@app.route('/', methods=["GET", "POST"])
def handle_form_submit():
    form_data = request.form
    print(form_data)
    query = construct_query(form_data)
    labels, values = spark_submit_query(query)
    return render_template('index.html', title='PC Users per 100ms', max=max(values) + 1, labels=labels, values=values)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)