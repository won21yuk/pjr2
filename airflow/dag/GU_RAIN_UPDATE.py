from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, struct, lit, concat, split, array, substring
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .master("yarn") \
    .appName("GU_RAIN_UPDATE") \
    .getOrCreate()

# MySQL 연결 설정
user = "root"
password = "1234"
url = "jdbc:mysql://localhost:3306/pjt2"
driver = "com.mysql.cj.jdbc.Driver"
dbtable = "GU_RAIN"

# 어제 날짜 가져오기
today = datetime.today()
yes = today - timedelta(1)
yes = str(yes.strftime("%Y-%m-%d"))
date = yes.replace('-', '')


# df 불러오기
df=spark.read.option('multiline', 'true').json(f'/user/ubuntu/gu_rain/gu_rain{date}.json')

# 기초 가공
df = df.select('*').where(substring(df['RAINGAUGE_NAME'], -2, 2) == '구청') \
    .withColumn('split', split(df['RECEIVE_TIME'], ' ')) \
    .withColumn('base_dt', col('split').getItem(0)) \
    .withColumn('base_tm', col('split').getItem(1)) \
    .drop('RAINGAUGE_CODE', 'RECEIVE_TIME', 'RAINGAUGE_NAME', 'split') \
    .selectExpr("RAINFALL10 as rain_amt", "GU_CODE as gu_cd", "base_dt", "base_tm", "GU_NAME as gu_name") \
    .sort('base_dt', 'base_tm')

# MySQL 적재(db : pjt2)
df.write.jdbc(url, dbtable,"append", properties={"driver": driver, "user": user, "password": password})