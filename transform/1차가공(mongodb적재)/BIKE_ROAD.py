import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, struct, lit, array
from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("yarn") \
    .appName("BIKE_ROAD") \
    .getOrCreate()

# 자전거도로 코드명 리스트
list = [ '19000002', '19000005', '19000006', '19000007', '19000008', '19000010', '19000011', '19000013', '19000014', '17001211' ]


# hdfs에서 불러오기
df = spark.read.option("multiline", "true").json("/user/ubuntu/bike_road/bike_19000001.json").drop('id', 'type')
df = df.withColumn('bike_road_name', df['properties'].ROAD_NAME) \
       .withColumn('coordinates', df['geometry'].coordinates[0]) \
       .drop('geometry', 'properties')


# multilinestring과 linestring 타입 구분하여 처리
for i in list:
	if i not in ['17001211', '19000007']:
		df1= spark.read.option("multiline", "true").json(f"/user/ubuntu/bike_road/bike_{i}.json").drop('id', 'type')
		df1 = df1.withColumn('bike_road_name', df1['properties'].ROAD_NAME) \
		         .withColumn('coordinates', df1['geometry'].coordinates[0]) \
		         .drop('geometry', 'properties')
		df = df.union(df1)
	else:
		df2 = spark.read.option("multiline", "true").json(f"/user/ubuntu/bike_road/bike_{i}.json").drop('id', 'type')
		df2 = df2.withColumn('bike_road_name', df2['properties'].ROAD_NAME) \
		         .withColumn('coordinates', df2['geometry'].coordinates) \
		         .drop('geometry', 'properties')
		df = df.union(df2)

# 기초 가공
df = df.withColumn('type', lit('Point')) \
		.select('*', struct(col('type'), col('coordinates')).alias('location')).drop('coordinates', 'type') \
		.selectExpr("bike_road_name",  "CAST(location AS struct<type:string,coordinates:array<array<float>>>) location")

# mongodb 적재(db : pjt2)
df.write.format('com.mongodb.spark.sql.DefaultSource') \
		.mode('overwrite') \
        .option('uri', 'mongodb://localhost:27017/pjt2.BIKE_ROAD') \
        .save()