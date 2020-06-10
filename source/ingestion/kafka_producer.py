import pickle
import random
import struct
from kafka import KafkaProducer

def random_gaussian_generator():
    num = random.gauss(10, 1)
    return struct.pack('f', num) # to bin

def serialize(data):
    return pickle.dumps(data)

def produce(generator, topic='topic'):
    bootstrap_servers = ['localhost:9092']
    topic_name = topic
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer = KafkaProducer()
    while True:
        event = generator.generate_data()
        print(serialize(event))
        ack = producer.send(topic_name, serialize(event))

if __name__ == "__main__":
    produce('record')