from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests, json, math
from pendulum import yesterday, today
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd
from pyspark import SparkContext
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id= 'DONG_POPUL_UPDATE',
    schedule_interval='@daily',
    start_date= today('Asia/Seoul')
)

key = '525a415a51766c613132306c5251794f'

tdy = datetime.today()

# api로 받아 올수 있는 가장 최신의 데이터가 당일 기준 6일 전
theday = tdy - timedelta(6)
sixdaysago = str(theday.strftime("%Y-%m-%d"))
stddate = sixdaysago.replace('-', '')



# DEF API 가져오는 함수 작성
def _get_buspopul():
    bus_popul_list = []
    startpg = 1
    endpg = 424
    url = f'http://openAPI.seoul.go.kr:8088/{key}/json/tpssEmdBus/{startpg}/{endpg}/{stddate}'
    response = requests.get(url)
    res_json = response.json()
    stat = res_json['tpssEmdBus']['row']
    bus_popul_list.extend(stat)
    with open(f'/home/ubuntu/dong_popul/bus_popul/bus_popul{stddate}.json', 'w', encoding='utf-8') as f:
        json.dump(bus_popul_list, f, indent=4, sort_keys=True, ensure_ascii=False)

def _get_subwaypopul():
    subway_popul_list = []
    startpg = 1
    endpg = 424
    url = f'http://openAPI.seoul.go.kr:8088/{key}/json/tpssSubwayPassenger/{startpg}/{endpg}/{stddate}'
    response = requests.get(url)
    res_json = response.json()
    stat = res_json['tpssSubwayPassenger']['row']
    subway_popul_list.extend(stat)
    with open(f'/home/ubuntu/dong_popul/sub_popul/sub_popul{stddate}.json', 'w', encoding='utf-8') as f:
        json.dump(subway_popul_list, f, indent=4, sort_keys=True, ensure_ascii=False)

def _get_lifepopul():
    startpg = 1
    endpg = 1000
    # 데이터 총 개수
    url = f'http://openapi.seoul.go.kr:8088/{key}/json/SPOP_LOCAL_RESD_DONG/{startpg}/{endpg}/{stddate}'
    resp_cn = requests.get(url)
    resp_cn_json = resp_cn.json()
    count = int(resp_cn_json['SPOP_LOCAL_RESD_DONG']['list_total_count'])
    count_num = math.trunc(count/1000) + 1
    life_pop_list = []
    for _ in range(count_num):
        url = f'http://openapi.seoul.go.kr:8088/{key}/json/SPOP_LOCAL_RESD_DONG/{startpg}/{endpg}/{stddate}'
        startpg = endpg + 1
        endpg += 1000

        response = requests.get(url)
        res_json = response.json()
        stat = res_json['SPOP_LOCAL_RESD_DONG']['row']
        life_pop_list.extend(stat)

    with open(f'/home/ubuntu/dong_popul/life_popul/life_popul{stddate}.json', 'w', encoding='utf-8') as f:
        json.dump(life_pop_list, f, indent=4, sort_keys=True, ensure_ascii=False)

# TASK

# TASK1. 시작 더미
start_task=DummyOperator(
    task_id='start_task',
    dag=dag
)

# TASK2. RAW DATA 받기
bus_popul_task = PythonOperator(
    task_id='bus_popul_task',
    python_callable = _get_buspopul,
    dag=dag
)

subway_popul_task = PythonOperator(
    task_id='subway_popul_task',
    python_callable = _get_subwaypopul,
    dag=dag
)

life_popul_task = PythonOperator(
    task_id='life_popul_task',
    python_callable = _get_lifepopul,
    dag=dag
)


# TASK3 : HDFS에 저장
data_upload_bus = BashOperator(
    task_id='data_upload_bus',
    bash_command= f'hdfs dfs -put /home/ubuntu/dong_popul/bus_popul/bus_popul{stddate}.json /user/ubuntu/dong_popul/bus_popul/bus_popul{stddate}.json',
)

data_upload_subway = BashOperator(
    task_id='data_upload_subway',
    bash_command= f'hdfs dfs -put /home/ubuntu/dong_popul/sub_popul/sub_popul{stddate}.json /user/ubuntu/dong_popul/sub_popul/sub_popul{stddate}.json',
)

data_upload_life = BashOperator(
    task_id='data_upload_life',
    bash_command= f'hdfs dfs -put /home/ubuntu/dong_popul/life_popul/life_popul{stddate}.json /user/ubuntu/dong_popul/life_popul/life_popul{stddate}.json',
)


# TASK4 : PYSPARK
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application="pyspark/DONG_POPUL_UPDATE.py",
    conn_id='spark_default',
    dag=dag
)

# TASK5. 종료 더미
end_task=DummyOperator(
    task_id='end_task',
    dag=dag
)

start_task >> [bus_popul_task, subway_popul_task, life_popul_task] 
bus_popul_task >> data_upload_bus
subway_popul_task >> data_upload_subway
life_popul_task >> data_upload_life
[data_upload_bus, data_upload_subway, data_upload_life] >> spark_submit_task >> end_task