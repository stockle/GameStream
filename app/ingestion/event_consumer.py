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
        consumer = KafkaConsumer(
            self.topic,
            group_id=self.group,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('ascii'))
        )
        try:
            for message in consumer:
                event = message.value
                self.handler(db, event)
        except KeyboardInterrupt:
            sys.exit()

if __name__ == "__main__":
    pass
