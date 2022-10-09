# EVENT_PLACE.py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, explode, struct, lit, concat, split, array, arrays_zip

spark = SparkSession.builder \
    .master("yarn") \
    .appName("EVENT_PLACE") \
    .getOrCreate()
    
festival = spark.read.option("encoding", "utf-8") \
                    .option("multiline", "true") \
                    .json("/user/ubuntu/festival.json") \
					.select(explode('culture').alias('temp')) \
					.select(col('temp.title').alias('event_nm'),
							col('temp.place').alias('place_nm'),
							col('temp.coord').alias('coord_xy')) \
					.select(col('event_nm'), 
							explode(arrays_zip(col('place_nm'), col('coord_xy')))) \
					.select(col('event_nm'), 
							col('col.place_nm'), 
							col('col.coord_xy')[1].alias('longitude'),
							col('col.coord_xy')[0].alias('latitude')) \
					.select(col('event_nm'),
							col('place_nm'),
							struct(lit('Point').alias('type'),
									array([col('longitude').cast('float'),
											col('latitude').cast('float')]) \
									.alias('coordinates')) \
							.alias('location'))
					
# mongodb pjt2 db에 적재
festival.write.format('com.mongodb.spark.sql.DefaultSource') \
					.mode('overwrite') \
					.option("uri", "mongodb://localhost:27017/pjt2.EVENT_PLACE") \
					.save()
