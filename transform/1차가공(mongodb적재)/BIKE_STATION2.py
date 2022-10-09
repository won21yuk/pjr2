from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, struct, lit, concat, split, array
from pyspark.sql.types import *

df = spark.read.format('csv') \
    .option('encoding', 'utf-8') \
    .option('header', 'true') \
    .load('/user/ubuntu/bike_station2.csv').drop('주소2', '_c0')

df = df.withColumn('type', lit('Point')) \
    .withColumn('bike_station_id', split(df['대여소_ID'], '-').getItem(1)) \
    .withColumn('longitude', col('경도').cast(FloatType())) \
    .withColumn('latitude', col('위도').cast(FloatType())) \
    .withColumn('dong_cd', col('동코드').cast(StringType())) \
    .drop('경도', '위도', '동코드', '대여소_ID') \
    .withColumnRenamed('주소1', 'station_addr') \
    .select('bike_station_id', 'station_addr', 'dong_cd', array([col('longitude'), col('latitude')]).alias('coordinates'), 'type') \
    .select('*', struct('type', 'coordinates').alias('location')).drop('type', 'coordinates')

df.write.format('com.mongodb.spark.sql.DefaultSource') \
    .mode('overwrite') \
    .option("uri", "mongodb://localhost:27017/pjt2.BIKE_STATION") \
    .save()