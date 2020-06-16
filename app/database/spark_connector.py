import os
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf

class SparkConnector:
	def __init__(self):
		conf = SparkConf \
			.setAppName('SparkCassandraAnalytics') \
			.set('spark.cassandra.connection.host', os.environ['DB_ADDR']) \
			.set('spark.cassandra.auth.username', os.environ['DB_USER']) \
			.set('spark.cassandra.auth.password', os.environ['DB_PASS']) \
			.getOrCreate()
		self.sc = CassandraSparkContext(conf=conf)
		self.sqlContext = SQLContext(sc)

	def submit_sql(self, query):
		return self.sqlContext.sql(query).collect()

	def load_and_get_table_df(self, keys_space_name, table_name):
		table_df = self.sqlContext.read\
			.format("org.apache.spark.sql.cassandra")\
			.options(table=table_name, keyspace=keys_space_name)\
			.load()
		return table_df