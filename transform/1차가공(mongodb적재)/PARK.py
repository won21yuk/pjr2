# PARK.py
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, explode, struct, lit, concat, split, array

spark = SparkSession.builder \
    .master("yarn") \
    .appName("PARK") \
    .getOrCreate()

park = spark.read.option("multiline", "true").json("/user/ubuntu/park.json") \
	.drop('RESULT', 'list_total_count') \
	.select(explode(col('row')).alias('temp')) \
	.select('temp.P_IDX', 'temp.P_PARK', 'temp.P_ZONE', 'temp.LONGITUDE', 'temp.LATITUDE') \
	.select("*", lit("Point").alias("type")) \
	.na.replace("", "0") \
	.select(col('P_IDX').alias('park_id'),
					col('P_PARK').alias('park_nm'), 
					col('P_ZONE').alias('park_gu'), 
					array([col('LONGITUDE').cast('float'), col('LATITUDE').cast('float')]).alias('coordinates'), 
					col('type')) \
	.select('*', struct(col('type'), col('coordinates')).alias('location')).drop('type', 'coordinates')

# mongodb pjt2 db에 적재
park.write.format('com.mongodb.spark.sql.DefaultSource') \
					.mode('overwrite') \
					.option("uri", "mongodb://localhost:27017/pjt2.PARK") \
					.save()
