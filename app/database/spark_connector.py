import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

class SparkConnector:
	def __init__(self):
		# os.environ['PYSPARK_SUBMIT_ARGS'] = f"""
  #       	--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0
  #   	"""
		conf = SparkConf() \
			.setMaster("local[*]") \
			.setAppName('SparkCassandraAnalytics') \
			.set('spark.cassandra.connection.host', os.environ['DB_ADDR']) \
			.set('spark.cassandra.auth.username', os.environ['DB_USER']) \
			.set('spark.cassandra.auth.password', os.environ['DB_PASS'])
		sc = SparkContext(conf=conf)
		self.sqlContext = SQLContext(sc)

	def submit_sql(self, query):
		return self.sqlContext.sql(query).collect()

	def load_and_get_table_df(self, keys_space_name, table_name):
		table_df = self.sqlContext.read \
			.format("org.apache.spark.sql.cassandra") \
			.options(table=table_name, keyspace=keys_space_name) \
			.load()
		return table_df
