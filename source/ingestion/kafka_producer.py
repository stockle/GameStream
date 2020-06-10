import json
import random
import struct
import msgpack
from copy import deepcopy
if __name__ != "__main__":
    from kafka import KafkaProducer

def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
    return obj

def produce(generator, topic='topic'):
    if __name__ != "__main__":
        bootstrap_servers = ['localhost:9092']
        topic_name = topic
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: msgpack.packb(v, default=encode_datetime, use_bin_type=True)
        )
    while True:
        event = generator.generate_data()
        print(msgpack.packb(event, default=encode_datetime, use_bin_type=True))
        if __name__ != "__main__":
            ack = producer.send(topic_name, event)

if __name__ == "__main__":
    import data_generator
    dg = data_generator.DataGenerator(100)
    produce(dg)
