from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, struct, lit, concat, split, array, substring, arrays_zip
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import monotonically_increasing_id
spark = SparkSession.builder.master("yarn").getOrCreate()

# MySQL 연결 설정
user = "root"
password = "1234"
url = "jdbc:mysql://localhost:3306/mysql"
driver = "com.mysql.cj.jdbc.Driver"
dbtable = "DONG_POPUL"

today = datetime.today()

# api로 받아 올수 있는 가장 최신의 데이터가 당일 기준 5일 전
theday = today - timedelta(6)
sixdaysago = str(theday.strftime("%Y-%m-%d"))
stddate = sixdaysago.replace('-', '')

# dong_code

df=spark.read.option('header', 'true').csv('/user/ubuntu/GU_CODE.csv')
DONG_CODE = df.selectExpr("H_SDNG_CD as dong_cd", 'H_DNG_CD as dong_cd_8', 'H_DNG_NM as dong_nm', 'RESD_CD as gu_cd')
DONG = DONG_CODE.toPandas()

# bus_popul spark

bus_popul = spark.read.option("multiline", "true").json(f"/user/ubuntu/dong_popul/bus_popul/bus_popul{stddate}.json")
temp_bus = bus_popul.toPandas()
columns = ['BUS_PSGR_CNT_00HH', 'BUS_PSGR_CNT_01HH', 'BUS_PSGR_CNT_02HH', 'BUS_PSGR_CNT_03HH', 'BUS_PSGR_CNT_04HH', 'BUS_PSGR_CNT_05HH',\
'BUS_PSGR_CNT_06HH', 'BUS_PSGR_CNT_07HH', 'BUS_PSGR_CNT_08HH', 'BUS_PSGR_CNT_09HH', 'BUS_PSGR_CNT_10HH', 'BUS_PSGR_CNT_11HH', 'BUS_PSGR_CNT_12HH', \
'BUS_PSGR_CNT_13HH', 'BUS_PSGR_CNT_14HH', 'BUS_PSGR_CNT_15HH', 'BUS_PSGR_CNT_16HH', 'BUS_PSGR_CNT_17HH',\
'BUS_PSGR_CNT_18HH', 'BUS_PSGR_CNT_19HH', 'BUS_PSGR_CNT_20HH', 'BUS_PSGR_CNT_21HH', 'BUS_PSGR_CNT_22HH', 'BUS_PSGR_CNT_23HH']
temp_bus = pd.melt(temp_bus, id_vars=["ADMDONG_ID", "CRTR_DT"], value_vars=columns, var_name='base_tm',value_name='bus_popul')
bus_popul = spark.createDataFrame(temp_bus)
bus_popul = bus_popul.selectExpr('CRTR_DT as base_dt', "ADMDONG_ID as dong_cd", 'base_tm', 'bus_popul')
bus_popul = bus_popul.select('base_dt', substring('base_tm', 14, 2).alias('base_tm'), 'dong_cd', 'bus_popul')
temp_bus = bus_popul.toPandas()
temp_bus = pd.merge(temp_bus, DONG, how='left', on='dong_cd')
bus_popul=spark.createDataFrame(temp_bus)
bus_popul = bus_popul.selectExpr('base_dt', 'base_tm', 'bus_popul', 'dong_cd_8 as dong_cd')

# subway_popul spark

subway_popul = spark.read.option("multiline", "true").json(f"/user/ubuntu/dong_popul/sub_popul/sub_popul{stddate}.json")
temp_sub = subway_popul.toPandas()
columns = ['SBWY_PSGR_CNT_00HH', 'SBWY_PSGR_CNT_01HH', 'SBWY_PSGR_CNT_02HH', 'SBWY_PSGR_CNT_03HH', 'SBWY_PSGR_CNT_04HH', 'SBWY_PSGR_CNT_05HH',\
'SBWY_PSGR_CNT_06HH', 'SBWY_PSGR_CNT_07HH', 'SBWY_PSGR_CNT_08HH', 'SBWY_PSGR_CNT_09HH', 'SBWY_PSGR_CNT_10HH', 'SBWY_PSGR_CNT_11HH', 'SBWY_PSGR_CNT_12HH', \
'SBWY_PSGR_CNT_13HH', 'SBWY_PSGR_CNT_14HH', 'SBWY_PSGR_CNT_15HH', 'SBWY_PSGR_CNT_16HH', 'SBWY_PSGR_CNT_17HH',\
'SBWY_PSGR_CNT_18HH', 'SBWY_PSGR_CNT_19HH', 'SBWY_PSGR_CNT_20HH', 'SBWY_PSGR_CNT_21HH', 'SBWY_PSGR_CNT_22HH', 'SBWY_PSGR_CNT_23HH']

temp_sub = pd.melt(temp_sub, id_vars=["ADMDONG_ID", "CRTR_DT"], value_vars=columns, var_name='base_tm',value_name='subway_popul')
subway_popul = spark.createDataFrame(temp_sub)
subway_popul = subway_popul.selectExpr('CRTR_DT as base_dt', "ADMDONG_ID as dong_cd", 'base_tm', 'subway_popul')
subway_popul = subway_popul.select('base_dt', substring('base_tm', 15, 2).alias('base_tm'), 'dong_cd', 'subway_popul')
temp_sub = subway_popul.toPandas()
temp_sub = pd.merge(temp_sub, DONG, how='left', on='dong_cd')
subway_popul=spark.createDataFrame(temp_sub)
subway_popul = subway_popul.selectExpr('base_dt', 'base_tm', 'subway_popul', 'dong_cd_8 as dong_cd')

# life_popul spark
life = spark.read.option("multiline", "true").json(f"/user/ubuntu/dong_popul/life_popul/life_popul{stddate}.json")
life_popul = life.select(col('ADSTRD_CODE_SE'), col('STDR_DE_ID'), col('TMZON_PD_SE'), col('TOT_LVPOP_CO')) \
        .sort('STDR_DE_ID') \
        .selectExpr("ADSTRD_CODE_SE as dong_cd", "STDR_DE_ID as base_dt", "TMZON_PD_SE as base_tm", "TOT_LVPOP_CO as life_popul") \
        .sort('base_dt', 'base_tm')

# DONG_POPUL
bus_popul.createOrReplaceTempView('bus_popul')
subway_popul.createOrReplaceTempView('subway_popul')

dong_popul = spark.sql("""
	select subway_popul.base_dt, subway_popul.base_tm, subway_popul.dong_cd, subway_popul.subway_popul, bus_popul.bus_popul
	from subway_popul inner join bus_popul
	where subway_popul.base_dt = bus_popul.base_dt and subway_popul.base_tm = bus_popul.base_tm and subway_popul.dong_cd = bus_popul.dong_cd
""")
life_popul.createOrReplaceTempView('life_popul')
dong_popul.createOrReplaceTempView('dong_popul')
dong_popul = spark.sql("""
	select dong_popul.base_dt, dong_popul.base_tm, dong_popul.dong_cd, dong_popul.subway_popul, dong_popul.bus_popul, life_popul.life_popul
	from dong_popul inner join life_popul
	where dong_popul.base_dt = life_popul.base_dt and dong_popul.base_tm = life_popul.base_tm and dong_popul.dong_cd = life_popul.dong_cd
""").sort('base_dt', 'base_tm')

dong_popul.write.jdbc(url, dbtable,"append", properties={"driver": driver, "user": user, "password": password})







