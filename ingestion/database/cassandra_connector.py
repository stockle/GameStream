import os
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, BatchStatement

DB_IDLE = 10

class DBConnector:
	def __init__(self):
		self.session = None
		self.cluster = None
		self.keyspace = None

	def __del__(self):
		self.cluster.shutdown()

	def init_cluster(self):
		ap = PlainTextAuthProvider(
			username=os.environ['DB_USER'],
			password=os.environ['DB_PASS']
		)
		self.cluster = Cluster([os.environ['DB_ADDR']], auth_provider=ap, idle_heartbeat_interval=DB_IDLE)

	def init_session(self):
		if not self.session:
			self.init_cluster()
			self.session = self.cluster.connect()

	def init_keyspace(self, keyspace):
		self.session.execute("""
			CREATE KEYSPACE IF NOT EXISTS %s
			WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
		""" % keyspace)
		self.session.set_keyspace(keyspace)
		self.session.execute('USE ' + keyspace)

	def execute(self, query):
		print(self.session.execute(query))

	def insert(self, query, data):
		batch = BatchStatement()
		for d in data:
			batch.add(query, d)
		print(self.session.execute(batch))

	def select(self, query):
		rows = self.session.execute(query)
		return rows

if __name__=="__main__":
	db = DBConnector()
	db.init_session()
	db.init_keyspace('test_space')

	query = """
		DROP TABLE IF EXISTS test
	"""
	db.execute(query)

	query = """
		CREATE TABLE IF NOT EXISTS test
		(id text PRIMARY KEY, name text);
	"""
	db.execute(query)

	query = """INSERT INTO test (id, name) VALUES (%s, %s)"""
	db.insert(query, [
		("1", "test")
	])

	query = """
		SELECT * FROM test;
	"""
	db.select(query)