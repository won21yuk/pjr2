# BIKE_STATION2.py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, struct, lit, concat, split, array
from pyspark.sql.types import *
import requests
import json

spark = SparkSession.builder \
    .master("yarn") \
    .appName("BIKE_STATION") \
    .getOrCreate()

# csv한글 깨짐현상 => encoding euc-kr option 적용시 해결
df = spark.read.format('csv') \
    .option('encoding', 'euc-kr') \
    .option('header', 'true') \
    .load('/user/ubuntu/BIKE_STATION.csv').drop('주소2')
# df_test = spark.read.option("encoding", "euc-kr").option("header", "true").csv("hdfs://localhost:9000/user/big/bike_road/BIKE_STATION.csv")

# spark -> pandas df
pdf = df.toPandas()

# 주소 -> 좌표 변환

row_num = int(len(pdf))
for i in range(row_num):
    location = pdf['주소1'][i]
    url = f"https://dapi.kakao.com/v2/local/search/keyword.json?query={location}"
    kakao_key = "eeb4d25bd0990160503da341e8678475"
    result = requests.get(url, headers={"Authorization": f"KakaoAK {kakao_key}"})
    json_obj = result.json()

    if pdf.loc[i, '위도'] == 0:
        try:
            x = json_obj['documents'][0]['x']
            y = json_obj['documents'][0]['y']
            pdf.loc[i, '경도'] = x
            pdf.loc[i, '위도'] = y
        except:
            pass

# 빈 열(동코드) 만들기
pdf['동코드'] = ""

## 좌표 -> 동코드
dong_cd = list()
for x, y in zip(pdf['경도'], pdf['위도']):
    kakao_key = "eeb4d25bd0990160503da341e8678475"
    url = f'https://dapi.kakao.com/v2/local/geo/coord2regioncode.json?x={x}&y={y}'
    try:
        resp = requests.get(url, headers={"Authorization" : f"KakaoAK {kakao_key}"})
        result = resp.text
        json_req = json.loads(result)['documents'][1]
        # 행정동 코드 8자리 가져오기
        dong_code = json_req.get('code')[:8]
    except:
        dong_code = 'NaN'
    dong_cd.append(dong_code)

# 대여소별 행정동 정보 리스트
pdf['동코드'] = dong_cd

# pandas -> spark df
df = spark.createDataFrame(pdf)

# 기초 가공
df = df.withColumn('type', lit('Point')) \
    .withColumn('bike_station_id', split(df['대여소_ID'], '-').getItem(1)) \
    .withColumn('longitude', col('경도').cast(FloatType())) \
    .withColumn('latitude', col('위도').cast(FloatType())) \
    .withColumn('dong_cd', col('동코드').cast(StringType())) \
    .drop('경도', '위도', '동코드', '대여소_ID') \
    .withColumnRenamed('주소1', 'station_addr') \
    .select('bike_station_id', 'station_addr', 'dong_cd', array([col('longitude'), col('latitude')]).alias('coordinates'), 'type') \
    .select('*', struct('type', 'coordinates').alias('location')).drop('type', 'coordinates')


# mongodb 적재(db : pjt2)
df.write.format('com.mongodb.spark.sql.DefaultSource') \
    .mode('overwrite') \
    .option("uri", "mongodb://localhost:27017/pjt2.BIKE_STATION") \
    .save()
