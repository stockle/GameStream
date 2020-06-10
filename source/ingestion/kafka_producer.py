import json
import random
import struct
from copy import deepcopy
if __name__ != "__main__":
    from kafka import KafkaProducer

def produce(generator, topic='topic'):
    if __name__ != "__main__":
        bootstrap_servers = ['localhost:9092']
        topic_name = topic
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    while True:
        event = generator.generate_data()
        print(event)
        if __name__ != "__main__":
            ack = producer.send(topic_name, event)

if __name__ == "__main__":
    import data_generator
    dg = data_generator.DataGenerator(100)
    produce(dg)