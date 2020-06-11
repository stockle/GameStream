import sys
import json
import pickle
import msgpack
import umsgpack
from ast import literal_eval
from datetime import datetime
from kafka import KafkaConsumer

def handle_event(db, event):
    query = """
        INSERT INTO events 
        (user_id, event_time, game, platform, platform_stats)
        VALUES (%s,%s,%s,%s,%s)
    """
    db.insert(query, [(
        event['UID'], event['Time'],
        event['event_body']['Game'],
        event['event_body']['Platform'],
        json.dumps(event['event_body']['PlatformStats'])
    )])

def decode_datetime(obj):
    if '__datetime__' in obj:
        obj = datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
    return obj

def consume(db, topic='topic'):
    bootstrap_servers = ['localhost:9092']
    topic_name = topic
    consumer = KafkaConsumer(
        topic_name,
        group_id='group1',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
    )
    try:
        for message in consumer:
            print(message.value.decode('utf-16'))
            event = json.loads(message.value.decode('utf-16'))
            print(event)
            handle_event(db, event)
    except KeyboardInterrupt:
        sys.exit()

if __name__ == "__main__":
    pass
