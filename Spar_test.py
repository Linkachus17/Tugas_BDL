from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.read.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "localhost:2020").option("spark.cassandra.auth.username", "cassandra").option("spark.cassandra.auth.password", "cassandra").option("keyspace", "data").option("table", "imdb").load()
df.show()
