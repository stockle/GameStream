import json
import random
import struct
import pickle
import umsgpack
from copy import deepcopy
from datetime import datetime
if __name__ != "__main__":
    from kafka import KafkaProducer

def encode_datetime(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y%m%dT%H:%M:%S.%f")
    return obj

def produce(generator, topic='topic'):
    if __name__ != "__main__":
        bootstrap_servers = ['localhost:9092']
        topic_name = topic
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    while True:
        event = generator.generate_data()
        if __name__ != "__main__":
            print(json.dumps(event, indent=4, sort_keys=True, default=str).encode('utf-8'))
            ack = producer.send(
                topic_name,
                pickle.dumps(event)
            )
            break

if __name__ == "__main__":
    import data_generator
    dg = data_generator.DataGenerator(100)
    produce(dg)
