import pickle
import random
import struct
import data_generator
from kafka import KafkaProducer

def serialize(data):
    return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

def produce(generator, topic='topic'):
    bootstrap_servers = ['localhost:9092']
    topic_name = topic
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer = KafkaProducer()
    while True:
        event = generator.generate_data()
        ack = producer.send(topic_name, serialize(event))

if __name__ == "__main__":
    produce('record')