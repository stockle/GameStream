import json
import logging
from random import seed
from datetime import datetime
from kafka import KafkaProducer
import event_producer, data_generator
from database import cassandra_connector

def produce(generator):
    bootstrap_servers = ['localhost:9092']
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )

    while True:
        event = generator.generate_data()
        ack = producer.send(
            event['event_type'],
            event
        )
        logging.info('Sent message ('+str(event['event_type'])+'):', str(ack.get().partition))

def get_age_bracket(age):
    min_age = 0
    max_age = 0
    if age < 20:
        return 13, 19
    elif age >= 75:
        max_age = 75
    min_age = age - (age % 10)
    if max_age == 0:
        max_age = min_age + 9
    return min_age, max_age

def create_users(db):
    query = """
        DROP TABLE IF EXISTS users
    """
    db.execute(query)

    query = """
        CREATE TABLE IF NOT EXISTS users
        (id text PRIMARY KEY, min_age int, max_age int);
    """
    db.execute(query)

def init_db(db):
    db.init_cluster()
    db.init_session()
    db.init_keyspace('v1')

def populate_db(db, users):
    create_users(db)

    qset = []
    for i, user in enumerate(users):
        min_age, max_age = get_age_bracket(user['Age'])
        qset.append((user['UID'], min_age, max_age))
        if i % 100 == 0:
            query = """INSERT INTO users (id, min_age, max_age) VALUES (%s, %s, %s)"""
            db.insert(query, qset)
            qset = []

if __name__=="__main__":
    seed()

    logging.basicConfig(
        filename='/tmp/gamestream-main-'
        + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        + '-.log',
        level=logging.DEBUG
    )
    
    datageni = data_generator.DataGenerator(101)
    db = cassandra_connector.DBConnector()
    kafka_producer = KafkaProducer()

    init_db(db)
    users = datageni.get_users()
    populate_db(db, users)

    produce(datageni)
