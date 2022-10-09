from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import requests, json, math
from pendulum import yesterday, today
from datetime import datetime, timedelta


# 대그 선언
dag = DAG(
    dag_id='GU_RAIN_UPDATE',
    schedule_interval='@daily',
    start_date= today('Asia/Seoul')
)

# 함수부
tdy = datetime.today()
yes = tdy - timedelta(1)
yesterday = str(yes.strftime("%Y-%m-%d"))
date = yesterday.replace('-', '')


def _get_gurain():
    startpg = 1
    endpg = 1000
    key = '525a415a51766c613132306c5251794f'
    url = f'http://openAPI.seoul.go.kr:8088/{key}/json/ListRainfallService/{startpg}/{endpg}/'
    resp_cn = requests.get(url)
    resp_cn_json = resp_cn.json()
    count = int(resp_cn_json['ListRainfallService']['list_total_count'])
    count_num = math.trunc(count/1000) + 1

    gu_rain_list = []
    for _ in range(11):
        url = f'http://openAPI.seoul.go.kr:8088/{key}/json/ListRainfallService/{startpg}/{endpg}/'
        startpg = endpg + 1
        endpg += 1000

        response = requests.get(url)
        res_json = response.json()
        stat = res_json['ListRainfallService']['row']
        print(stat)
        #input_dict = json.load(stat)

        filter_date = [x for x in stat if x['RECEIVE_TIME'].split(' ')[0] == yesterday]
        if not filter_date:
            pass
        else:
            gu_rain_list.extend(filter_date)

    with open(f'/home/ubuntu/gu_rain/gu_rain{date}.json', 'w', encoding='utf-8') as f:
        json.dump(gu_rain_list, f, indent=4, sort_keys=True, ensure_ascii=False)



# task
# 1. 시작 더미
start_task=DummyOperator(
    task_id='start_task',
    dag=dag
)

# 2. gu_rain 로컬에 다운받기
get_gurain_task = PythonOperator(
    task_id='get_gurain_task',
    python_callable=_get_gurain,
    dag=dag
)

# 3. hdfs에 저장
data_upload_task = BashOperator(
    task_id='data_upload_task',
    bash_command=f'hdfs dfs -put /home/ubuntu/gu_rain/gu_rain{date}.json /user/ubuntu/gu_rain/gu_rain{date}.json',
    dag=dag
)

# template 축약어 옵션 : {{ yesterday_ds_nodash }}

# 4. 가공 및 db 적재
spark_submit_task=SparkSubmitOperator(
    task_id='spark_submit_task',
    application='pyspark/GU_RAIN_UPDATE.py',
    conn_id='spark_default',
    dag=dag
)

# 5. 종료 더미
end_task=DummyOperator(
    task_id='end_task',
    dag=dag
)


# 디펜던시
start_task >> get_gurain_task >> data_upload_task >> spark_submit_task >> end_task
