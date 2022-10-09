import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, struct, lit, concat, split, array
from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("yarn") \
    .appName("SUBWAY_STATION") \
    .getOrCreate()


df = spark.read.json('/user/ubuntu/subwayinfo.json').drop('RESULT', 'list_total_count')

# 기초가공 및 geojson 형태 변환
df_row = df.select(explode(col('row')).alias('row'))
sub_df = df_row.select('row.STATN_ID', 'row.STATN_NM', 'row.CRDNT_X', 'row.CRDNT_Y')
sub_df = sub_df.withColumn('type', lit('Point')) \
				.withColumn('longitude', col('CRDNT_X').cast(FloatType())) \
				.withColumn('latitude', col('CRDNT_Y').cast(FloatType())) \
				.drop('CRDNT_X', 'CRDNT_Y') \
				.select(col('STATN_ID').alias('subway_id'), col('STATN_NM').alias('subway_station_name'), array(['longitude', 'latitude']).alias('coordinates'), 'type') \
				.drop('latitude', 'longitude') \
				.select('*', struct('type', 'coordinates').alias('location')).drop('type', 'coordinates')
 

# mongodb 적재(db : pjt2)
sub_df.write.format('com.mongodb.spark.sql.DefaultSource') \
			.mode('overwrite') \
	        .option('uri', 'mongodb://localhost:27017/pjt2.SUBWAY_STATION') \
		    .save()