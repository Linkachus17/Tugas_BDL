from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Datacamp Pyspark Tutorial").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()
df = spark.read.csv('datacamp_ecommerce.csv',header=True,escape="\"")
df.show(5,0)
df.count()  # Answer: 2,500
df.select('CustomerID').distinct().count() # Answer: 95
df.groupBy('Country').agg(countDistinct('CustomerID').alias('country_count')).show()
df.groupBy('Country').agg(countDistinct('CustomerID').alias('country_count')).orderBy(desc('country_count')).show()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df = df.withColumn('date',to_timestamp("InvoiceDate", 'yy/MM/dd HH:mm'))
df.select(max("date")).show()
df.select(min("date")).show()