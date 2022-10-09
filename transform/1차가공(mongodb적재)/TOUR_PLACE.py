# TOUR_PLACE.py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, explode, struct, lit, concat, split, array

spark = SparkSession.builder \
    .master("yarn") \
    .appName("TOUR_PLACE") \
    .getOrCreate()

tour_place = spark.read.option("encoding", "utf-8").csv("/user/ubuntu/seoul_attractions.csv") \
			.toDF('place_id', 'place_nm', 'place_star', 'latitude', 'longitude') \
			.na.replace("", "0").na.fill("0") \
			.select(col('place_id'), 
					col('place_nm'), 
					struct(lit('Point').alias('type'), 			
							array([col('longitude').cast('float'),
									col('latitude').cast('float')]) \
							.alias('coordinates')) \
					.alias('location'))

# mongodb pjt2 db에 적재
tour_place.write.mode('overwrite').format('com.mongodb.spark.sql.DefaultSource') \
					.option("uri", "mongodb://localhost:27017/pjt2.TOUR_PLACE") \
					.save()
