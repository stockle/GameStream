from random import seed
from threading import Thread
from database import connector
from ingestion import \
	event_consumer, kafka_producer, \
	data_generator, consumers

def init_db(db):
	db.init_cluster()
	db.init_session()
	db.init_keyspace('v1')

def create_users(db, users):
	query = """
		DROP TABLE IF EXISTS users
	"""
	db.execute(query)

	query = """
		CREATE TABLE IF NOT EXISTS users
		(id text PRIMARY KEY, age int);
	"""
	db.execute(query)

	qset = []
	for i, user in enumerate(users):
		qset.append((user['UID'], user['Age']))
		if i % 100 == 0:
			query = """INSERT INTO users (id, age) VALUES (%s, %s)"""
			db.insert(query, qset)
			qset = []

def create_gameplay_events(db):
	query = """DROP TABLE IF EXISTS gameplay_events"""
	db.execute(query)

	query = """
		CREATE TABLE IF NOT EXISTS gameplay_events
		(
			PRIMARY KEY(user_id, event_time),
			user_id text, event_time timestamp,
			game text, platform text,
			platform_stats text
		);
	"""
	db.execute(query)

def create_purchase_events(db):
	query = """DROP TABLE IF EXISTS purchase_events"""
	db.execute(query)

	query = """
		CREATE TABLE IF NOT EXISTS purchase_events
		(
			PRIMARY KEY(user_id, event_time, item),
			user_id text, event_time timestamp,
			game text, platform text, item text,
			price decimal
		);
	"""
	db.execute(query)

def populate_db(db, users):
	create_users(db, users)
	create_purchase_events(db)
	create_gameplay_events(db)

def simulate(db, datageni):
	purchase_evcon = event_consumer.EventConsumer(db, consumers.consumer_purchase_event.handle_purchase_event, 'events', 'purchase_event')
	purchase_consumer = Thread(target=purchase_evcon.consume, args=( ))
	purchase_consumer.start()

	gameplay_evcon = event_consumer.EventConsumer(db, consumers.handle_ganeplay_event, 'events', 'gameplay_event')
	gameplay_consumer = Thread(target=gameplay_evcon.consume, args=( ))
	gameplay_consumer.start()

	kafka_producer.produce(datageni)

if __name__=="__main__":
	seed()
	datageni = data_generator.DataGenerator(100)
	db = connector.DBConnector()

	init_db(db)
	users = datageni.get_users()

	populate_db(db, users)
	simulate(db, datageni)