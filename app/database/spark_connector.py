import os
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

class SparkConnector:
	def __init__(self):
		self.sc = SparkContext("local", "spark_analytics")
		self.sqlContext = SQLContext(sc)

	def submit_sql(self, query):
		return self.sqlContext.sql(query).collect()

	def load_and_get_table_df(self, keys_space_name, table_name):
		table_df = self.sqlContext.read\
			.format("org.apache.spark.sql.cassandra")\
			.options(table=table_name, keyspace=keys_space_name)\
			.load()
		return table_df