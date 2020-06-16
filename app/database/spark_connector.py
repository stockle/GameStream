import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark_cassandra import CassandraSparkContext

class SparkConnector:
	def __init__(self):
		conf = SparkConf() \
			.setMaster("local[*]") \
			.setAppName('SparkCassandraAnalytics') \
			.set('spark.cassandra.connection.host', os.environ['DB_ADDR']) \
			.set('spark.cassandra.auth.username', os.environ['DB_USER']) \
			.set('spark.cassandra.auth.password', os.environ['DB_PASS'])
		sc = SparkContext(conf=conf)
		self.sqlContext = SQLContext(sc)
		self.submit_sql("""
			CREATE TEMPORARY VIEW gameplay_events
			USING org.apache.spark.sql.cassandra
			OPTIONS (
				table "gameplay_events",
				keyspace "v1"
			);
		""")

		self.submit_sql("""
			CREATE TEMPORARY VIEW purchase_events
			USING org.apache.spark.sql.cassandra
			OPTIONS (
				table "purchase_events",
				keyspace "v1"
			);
		""")

		self.submit_sql("""
			CREATE TEMPORARY VIEW users
			USING org.apache.spark.sql.cassandra
			OPTIONS (
				table "users",
				keyspace "v1"
			);
		""")

	def submit_sql(self, query):
		return self.sqlContext.sql(query).collect()

	def load_and_get_table_df(self, keys_space_name, table_name):
		table_df = self.sqlContext.read\
			.format("org.apache.spark.sql.cassandra")\
			.options(table=table_name, keyspace=keys_space_name)\
			.load()
		return table_df