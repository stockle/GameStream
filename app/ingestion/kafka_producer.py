import json
import random
import struct
import pickle
import umsgpack
from copy import deepcopy
from datetime import datetime
if __name__ != "__main__":
    from kafka import KafkaProducer, partitions_for

def encode_datetime(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y%m%dT%H:%M:%S.%f")
    return obj

def produce(generator):
    if __name__ != "__main__":
        bootstrap_servers = ['localhost:9092']
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda m: json.dumps(m).encode('ascii')
        )
    while True:
        event = generator.generate_data()
        print(partitions_for('events'))
        if __name__ != "__main__":
            ack = producer.send(
                event['event_type'],
                event
            )
            break

if __name__ == "__main__":
    import data_generator
    dg = data_generator.DataGenerator(100)
    produce(dg)
