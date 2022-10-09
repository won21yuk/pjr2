# SCHOOL.py (공식 json 파일)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, explode, struct, lit, concat, split, array

spark = SparkSession.builder \
    .master("yarn") \
    .appName("SCHOOL") \
    .getOrCreate()

df1 = spark.read.option("encoding", "utf-8") \
			.option("multiline", "true").json("/user/ubuntu/school.json") \
			.drop('fields') \
			.select(explode(col('records')).alias('temp')) \
			.select(col('temp.학교급구분').alias('school_kb'),
				col('temp.학교명').alias('school_nm'),
				col('temp.경도').alias('longitude'),
				col('temp.위도').alias('latitude')) \
			.na.replace("", "0")
			
df2 = spark.read.option("encoding", "euc-kr") \
			.option("multiline", "true").json("/user/ubuntu/university_loc.json") \
			.select('SebcCollegeInfoKor.row') \
        	.select(explode(col('row')).alias('temp')) \
      		.select(lit('대학교').alias('school_kb'),
      				col('temp.NAME_KOR').alias('school_nm'),
      				col('temp.`경도`').alias('longitude'),
      				col('temp.`위도`').alias('latitude')) \
      		.na.replace("", "0")
school = df1.unionByName(df2) \
		.select('*', lit('Point').alias('type'), 	
				array([col('longitude').cast('float'), 	
					col('latitude').cast('float')]).alias('coordinates')) \
		.drop('longitude', 'latitude') \
		.select(col('school_kb'), 
				col('school_nm'), 
				struct(col('type'), col('coordinates')).alias('location'))
				
# mongodb pjt2 db에 적재
school.write.format('com.mongodb.spark.sql.DefaultSource') \
					.mode('overwrite') \
					.option("uri", "mongodb://localhost:27017/pjt2.SCHOOL") \
					.save()
