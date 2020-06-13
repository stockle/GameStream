import numpy
import pandas as pd
from database import connector
from flask import Flask, Markup, render_template

app = Flask(__name__)

db = connector.DBConnector()
db.init_session()
db.init_keyspace('v1')

values = db.select("""
    SELECT * FROM gameplay_events
    WHERE platform = 'PC'
    ALLOW FILTERING;
""")

line_values = []
df = pd.DataFrame(list(values))
df['event_time'].astype('datetime64')
lines = df.groupby(pd.Grouper(key='event_time', freq='100ms')).event_time.agg('count').to_frame('count').reset_index()
line_values = lines['count'].values
labels = lines['event_time'].values

print(labels)

colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]

@app.route('/line')
def line():
    line_labels=labels
    return render_template('line_chart.html', title='Users per 100ms', max=max(line_values), labels=line_labels, values=line_values)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)