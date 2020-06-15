import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
# spark.cassandra.auth.password={os.environ['DB_PASS']}
# spark.cassandra.auth.username={os.environ['DB_USER']}

os.environ['PYSPARK_SUBMIT_ARGS'] = f"""
	--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0
	--conf
		spark.cassandra.connection.host={os.environ['DB_ADDR']}
	pyspark-shell
	"""

sc = SparkContext("local", "spark_analytics")
sqlContext = SQLContext(sc)

def load_and_get_table_df(keys_space_name, table_name):
	table_df = sqlContext.read\
		.format("org.apache.spark.sql.cassandra")\
		.options(table=table_name, keyspace=keys_space_name)\
		.load()
	return table_df

gevents = load_and_get_table_df("v1", "gameplay_events")
gevents.show()
