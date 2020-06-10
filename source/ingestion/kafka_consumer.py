import sys
import json
import pickle
from ast import literal_eval
from kafka import KafkaConsumer

def handle_event(db, event):
    query = """
        INSERT INTO events 
        (user_id, event_time, game, platform, platform_stats)
        VALUES (%s,%s,%s,%s,%s)
    """
    db.insert(query, [(
        event['UID'], datetime.strptime(event['Time'], "%Y-%m-%d %H:%M:%S.%f"),
        event['event_body']['Game'], event['event_body']['Platform'],
        json.dumps(event['event_body']['PlatformStats'])
    )])

def consume(db, topic='topic'):
    bootstrap_servers = ['localhost:9092']
    topic_name = topic
    consumer = KafkaConsumer(
        topic_name,
        group_id='group1',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest'
    )
    try:
        for message in consumer:
            event = json.loads(message.value.decode('utf-8'))
            handle_event(db, event)
    except KeyboardInterrupt:
        sys.exit()

if __name__ == "__main__":
    pass
