from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from pendulum import yesterday

dag=DAG(
    dag_id='DELETE',
    schedule_interval='@monthly',
    start_date= yesterday('Asia/Seoul')
)

# task 1
start=DummyOperator(
    task_id='start',
    dag=dag
)

# task 2
gurain_local_clear=BashOperator(
    task_id='gurain_local_clear',
    bash_command='rm -r /home/ubuntu/gu_rain/gu_rain*.json',
    dag=dag
)

lifepop_local_clear=BashOperator(
    task_id='lifepop_local_clear',
    bash_command='rm -r /home/ubuntu/dong_popul/life_popul/life_popul*.json',
    dag=dag
)

subpop_local_clear=BashOperator(
    task_id='subpop_local_clear',
    bash_command='rm -r /home/ubuntu/dong_popul/sub_popul/sub_popul*.json',
    dag=dag
)

buspop_local_clear=BashOperator(
    task_id='buspop_local_clear',
    bash_command='rm -r /home/ubuntu/dong_popul/bus_popul/bus_popul*.json',
    dag=dag
)


# task3
gurain_hdfs_clear=BashOperator(
    task_id='gurain_hdfs_clear',
    bash_command='hdfs dfs -rm -r /user/ubuntu/gu_rain/gu_rain*.json',
    dag=dag
)

lifepop_hdfs_clear=BashOperator(
    task_id='lifepop_hdfs_clear',
    bash_command='hdfs dfs -rm -r /user/ubuntu/dong_popul/life_popul/life_popul*.json',
    dag=dag
)

subpop_hdfs_clear=BashOperator(
    task_id='subpop_hdfs_clear',
    bash_command='hdfs dfs -rm -r /user/ubuntu/dong_popul/sub_popul/sub_popul*.json',
    dag=dag
)

buspop_hdfs_clear=BashOperator(
    task_id='buspop_hdfs_clear',
    bash_command='hdfs dfs -rm -r /user/ubuntu/dong_popul/bus_popul/bus_popul*.json',
    dag=dag
)

# task 4
end=DummyOperator(
    task_id='end',
    dag=dag
)

# 디펜던시
start >> [gurain_local_clear, lifepop_local_clear, subpop_local_clear, buspop_local_clear] 
gurain_local_clear >> gurain_hdfs_clear
lifepop_local_clear >> lifepop_hdfs_clear
subpop_local_clear >> subpop_hdfs_clear
buspop_local_clear >> buspop_hdfs_clear
[gurain_hdfs_clear, lifepop_hdfs_clear, subpop_hdfs_clear, buspop_hdfs_clear] >> end