# CULTURE_PLACE.py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, explode, struct, lit, concat, split, array

spark = SparkSession.builder \
    .master("yarn") \
    .appName("CULTURE_PLACE") \
    .getOrCreate()
    
culture_place = spark.read.option("encoding", "utf-8") \
                    .option("multiline", "true") \
                    .json("/user/ubuntu/culture_place.json") \
                    .select(explode(col('culturalSpaceInfo.row')).alias('temp')) \
                    .select(col('temp.SUBJCODE').alias('place_kb'),
                            col('temp.FAC_NAME').alias('place_nm'),
                            lit('Point').alias('type'),
                            col('temp.Y_COORD').alias('longitude'),
                            col('temp.X_COORD').alias('latitude')) \
                    .na.replace("", "0").na.fill("0") \
                    .select(col('place_kb'), col('place_nm'),
                            struct(col('type'),
                                    array([col('longitude').cast('float'),
                                            col('latitude').cast('float')]) \
                                    .alias('coordinates')) \
                                    .alias('location'))


culture_place.write.format('com.mongodb.spark.sql.DefaultSource') \
                        .mode('overwrite') \
                        .option("uri", "mongodb://localhost:27017/pjt2.CULTURE_PLACE") \
                        .save()
