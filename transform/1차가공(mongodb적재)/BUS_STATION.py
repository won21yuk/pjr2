import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, struct, lit, concat, split, array
from pyspark.sql.types import *


spark = SparkSession.builder \
    .master("yarn") \
    .appName("BUS_STATION") \
    .getOrCreate()


# hdfs에서 불러오기
df = spark.read.json('/user/ubuntu/businfo.json').drop('RESULT', 'list_total_count')

# 기초 가공
bus_df = df.select('STTN_ID', 'STTN_NM', 'CRDNT_X', 'CRDNT_Y')

bus_df = bus_df.withColumn('type', lit('Point')) \
				.withColumn('longitude', col('CRDNT_X').cast(FloatType())) \
				.withColumn('latitude', col('CRDNT_Y').cast(FloatType())) \
				.drop('CRDNT_X', 'CRDNT_Y') \
				.select(col('STTN_ID').alias('bus_id'), col('STTN_NM').alias('bus_station_name'), array(['longitude', 'latitude']).alias('coordinates'), 'type') \
				.drop('latitude', 'longitude') \
				.select('*', struct('type', 'coordinates').alias('location')) \
				.drop('type', 'coordinates')


# mongodb 적재(db : pjt2)
bus_df.write.format('com.mongodb.spark.sql.DefaultSource') \
		    .mode('overwrite') \
		    .option('uri', 'mongodb://localhost:27017/pjt2.BUS_STATION') \
		    .save()

