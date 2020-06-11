import sys
import json
import pickle
import msgpack
import umsgpack
from ast import literal_eval
from datetime import datetime
from kafka import KafkaConsumer

def decode_datetime(obj):
    if '__datetime__' in obj:
        obj = datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
    return obj

class EventConsumer:
    def __init__(self, db, handler, topic='topic', group='group_1'):
        self.db = db
        self.group = group
        self.topic = topic
        self.handler = handler

    def consume(self):
        bootstrap_servers = ['localhost:9092']
        topic_name = topic
        consumer = KafkaConsumer(
            topic_name,
            group_id=self.group,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
        )
        try:
            for message in consumer:
                event = pickle.loads(message.value)
                self.handler(db, event)
        except KeyboardInterrupt:
            sys.exit()

if __name__ == "__main__":
    pass