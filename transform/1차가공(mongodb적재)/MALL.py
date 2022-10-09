# MALL.py
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, explode, struct, lit, concat, split, array
from pyproj import Proj, transform
import pandas as pd

spark = SparkSession.builder \
    .master("yarn") \
    .appName("MALL") \
    .getOrCreate()

mall = spark.read.option("multiline", "true").json("/user/ubuntu/mall.json") \
        .drop('RESULT', 'list_total_count') \
        .select(explode(col('row')).alias('temp')) \
        .select(col('temp.MGTNO').alias('mall_id'),
        		col('temp.BPLCNM').alias('mall_nm'),
        		col('temp.TRDSTATENM').alias('status_kb'),
        		col('temp.UPTAENM').alias('uptae_kb'), 
        		col('temp.X').alias('x'), 
        		col('temp.Y').alias('y'), lit('Point').alias('type')) \
        .na.replace("", "0")

# 좌표 변환
coor_temp = mall.select(col('mall_id'), col('x'), col('y')).toPandas()
coor_temp = coor_temp.astype({'x':'float', 'y':'float'})
proj_1 = Proj(init='epsg:2097')
proj_2 = Proj(init='epsg:4326')
converted = transform(proj_1, proj_2, coor_temp.x, coor_temp.y)
coor_temp['lon'] = converted[0]
coor_temp['lat'] = converted[1]

location = coor_temp[['mall_id', 'lon', 'lat']]
location = spark.createDataFrame(location) \
				.select(col('mall_id'), 
						array([col('lon'), col('lat')]).alias('coordinates'))

mall = mall.join(location, on='mall_id') \
			.drop('x', 'y') \
			.select('*', struct(col('type'), col('coordinates')).alias('location')) \
			.drop('type', 'coordinates')
			
# mongodb pjt2 db에 적재
mall.write.format('com.mongodb.spark.sql.DefaultSource') \
					.mode('overwrite') \
					.option("uri", "mongodb://localhost:27017/pjt2.MALL") \
					.save()
