import json
import random
import struct
from copy import deepcopy
if __name__ != "__main__":
    from kafka import KafkaProducer


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()
        return super(DateTimeEncoder, self).default(obj)

def produce(generator, topic='topic'):
    if __name__ != "__main__":
        bootstrap_servers = ['localhost:9092']
        topic_name = topic
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        producer = KafkaProducer()
    while True:
        event = generator.generate_data()
        if __name__ != "__main__":
            ack = producer.send(topic_name, json.dumps(event, default=str))

if __name__ == "__main__":
    import data_generator
    dg = data_generator.DataGenerator(100)
    produce(dg)