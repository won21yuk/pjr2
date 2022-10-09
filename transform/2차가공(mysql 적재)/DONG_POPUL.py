from pyspark.sql.functions import lit, explode, col, arrays_zip, substring
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .master("yarn") \
    .appName("DONG_POPUL") \
    .getOrCreate()

# MySQL 연결 설정
user = "root"
password = "1234"
url = "jdbc:mysql://localhost:3306/pjt2"
driver = "com.mysql.cj.jdbc.Driver"
dbtable = "DONG_POPUL"

# 지하철 유동인구
df_sub = spark.read.option("multiline", "true").json("/home/ubuntu/sub_popul202208.json")

df_sub = df_sub.withColumnRenamed('CRTR_DT', 'base_dt') \
            .withColumnRenamed('SBWY_PSGR_CNT_00HH', '00HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_01HH', '01HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_02HH', '02HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_03HH', '03HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_04HH', '04HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_05HH', '05HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_06HH', '06HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_07HH', '07HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_08HH', '08HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_09HH', '09HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_10HH', '10HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_11HH', '11HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_12HH', '12HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_13HH', '13HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_14HH', '14HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_15HH', '15HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_16HH', '16HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_17HH', '17HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_18HH', '18HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_19HH', '19HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_20HH', '20HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_21HH', '21HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_22HH', '22HH_CNT') \
            .withColumnRenamed('SBWY_PSGR_CNT_23HH', '23HH_CNT') \
            .withColumnRenamed('ADMDONG_ID', 'DONG_ID')

pdf_sub = df_sub.toPandas()
columns = ['00HH_CNT', '01HH_CNT', '02HH_CNT', '03HH_CNT', '04HH_CNT', '05HH_CNT', '06HH_CNT', '07HH_CNT', '08HH_CNT', '09HH_CNT', '10HH_CNT', '11HH_CNT', '12HH_CNT', '13HH_CNT', '14HH_CNT', '15HH_CNT', '16HH_CNT', '17HH_CNT', '18HH_CNT','19HH_CNT', '20HH_CNT', '21HH_CNT', '22HH_CNT', '23HH_CNT']
pdf_sub = pd.melt(pdf_sub, id_vars=["DONG_ID", "base_dt"], value_vars=columns, var_name='base_tm',value_name='subway_popul')

df_sub = spark.createDataFrame(pdf_sub)
df_sub = df_sub.selectExpr("base_dt", "DONG_ID as dong_cd", 'base_tm', 'subway_popul') \
                .select('base_dt', substring('base_tm', 1, 2).alias('base_tm'), 'dong_cd', 'subway_popul')

# 버스 유동인구
df_bus = spark.read.option("multiline", "true").json("/home/ubuntu/bus_popul202208.json")

df_bus = df_bus.withColumnRenamed('CRTR_DT', 'base_dt') \
                .withColumnRenamed('BUS_PSGR_CNT_00HH', '00HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_01HH', '01HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_02HH', '02HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_03HH', '03HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_04HH', '04HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_05HH', '05HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_06HH', '06HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_07HH', '07HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_08HH', '08HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_09HH', '09HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_10HH', '10HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_11HH', '11HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_12HH', '12HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_13HH', '13HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_14HH', '14HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_15HH', '15HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_16HH', '16HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_17HH', '17HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_18HH', '18HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_19HH', '19HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_20HH', '20HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_21HH', '21HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_22HH', '22HH_CNT') \
                .withColumnRenamed('BUS_PSGR_CNT_23HH', '23HH_CNT') \
                .withColumnRenamed('ADMDONG_ID', 'DONG_ID')

pdf_bus = df_bus.toPandas()
columns = ['00HH_CNT', '01HH_CNT', '02HH_CNT', '03HH_CNT', '04HH_CNT', '05HH_CNT', '06HH_CNT', '07HH_CNT', '08HH_CNT', '09HH_CNT', '10HH_CNT', '11HH_CNT', '12HH_CNT', '13HH_CNT', '14HH_CNT', '15HH_CNT', '16HH_CNT', '17HH_CNT', '18HH_CNT','19HH_CNT', '20HH_CNT', '21HH_CNT', '22HH_CNT', '23HH_CNT']
pdf_bus = pd.melt(pdf_bus, id_vars=["DONG_ID", "base_dt"], value_vars=columns, var_name='base_tm',value_name='bus_popul')

df_bus = spark.createDataFrame(pdf_bus)
df_bus = df_bus.selectExpr("base_dt", "DONG_ID as dong_cd", 'base_tm', 'bus_popul') \
                .select('base_dt', substring('base_tm', 1, 2).alias('base_tm'), 'dong_cd', 'bus_popul')

# 생활인구
df_life = spark.read.option("multiline", "true").json("/user/ubuntu/life_popul202208.json")
df_life = df_life.select(col('ADSTRD_CODE_SE'), col('STDR_DE_ID'), col('TMZON_PD_SE'), col('TOT_LVPOP_CO')) \
            .sort('STDR_DE_ID') \
            .selectExpr("ADSTRD_CODE_SE as dong_cd", "STDR_DE_ID as base_dt", "TMZON_PD_SE as base_tm", "TOT_LVPOP_CO as life_popul") \
            .sort('base_dt', 'base_tm')

# 동 코드
df_dong=spark.read.option('header', 'true').csv('GU_CODE.csv')
GU_CODE =df_dong.selectExpr("RESD_CD as gu_cd", "CT_NM as gu_nm").distinct()
DONG_CODE = df_dong.selectExpr("H_SDNG_CD as dong_cd_7", 'H_DNG_CD as dong_cd_8', 'H_DNG_NM as dong_nm', 'RESD_CD as gu_cd')

# 버스 + 지하철 이용인구
df_bus.createOrReplaceTempView('bus_popul')
df_sub.createOrReplaceTempView('subway_popul')

dong_popul = spark.sql("""
	select subway_popul.base_dt, subway_popul.base_tm, subway_popul.dong_cd, subway_popul.subway_popul, bus_popul.bus_popul
	from subway_popul inner join bus_popul
	where subway_popul.base_dt = bus_popul.base_dt and subway_popul.base_tm = bus_popul.base_tm and subway_popul.dong_cd = bus_popul.dong_cd
""")

# 종합
DONG = DONG_CODE.toPandas()
temp = dong_popul.toPandas()
temp = pd.merge(temp, DONG, how='left', on='dong_cd')
dong_popul = spark.createDataFrame(temp)
dong_popul = dong_popul.selectExpr('base_dt', 'base_tm', 'dong_cd_8 as dong_cd', 'bus_popul', 'subway_popul')
dong_popul = dong_popul.withColumn('row_id', monotonically_increasing_id())
dong_popul = spark.sql("""
    select dong_popul.base_dt, dong_popul.base_tm, dong_popul.dong_cd, dong_popul.subway_popul, dong_popul.bus_popul, df_life.life_popul
    from dong_popul inner join df_life
    where dong_popul.base_dt = df_life.base_dt and dong_popul.base_tm = df_life.base_tm and dong_popul.dong_cd = df_life.dong_cd
""")

DONG_POPUL = dong_popul.select('row_id', 'base_dt', 'base_tm', 'dong_cd', 'life_popul', 'bus_popul', 'subway_popul')

# MySQL 적재(db : pjt2)
DONG_POPUL.write.jdbc(url, dbtable, "append", properties={"driver": driver, "user": user, "password": password})

