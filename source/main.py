from random import seed
from threading import Thread
from database import connector
from ingestion import kafka_consumer, kafka_producer, data_generator

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

def create_events(db):
	query = """DROP TABLE IF EXISTS events"""
	db.execute(query)

	query = """
		CREATE TABLE IF NOT EXISTS events
		(
			PRIMARY KEY(user_id, event_time),
			user_id int, event_time timestamp,
			game_id int, platform text,
			platform_stats text
		);
	"""
	db.execute(query)

def populate_db(db, users):
	create_users(db, users)
	create_events(db)

def simulate(db, datageni):
	topic = 'event'
	consumer = Thread(target=kafka_consumer.consume, args=(db, topic))
	consumer.start()
	kafka_producer.produce(datageni, topic)

if __name__=="__main__":
	seed()
	datageni = data_generator.DataGenerator(5)
	db = connector.DBConnector()

	init_db(db)
	users = datageni.get_users()

	populate_db(db, users)
	simulate(db, datageni)