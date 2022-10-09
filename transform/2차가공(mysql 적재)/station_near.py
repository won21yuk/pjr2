from pymongo import MongoClient
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import pymysql

connect = pymysql.connect(host='localhost', user='root', passwd='1234', db='pjt2')
cursor = connect.cursor()
cursor.execute(query='show tables')
table_list = []

# station_near는 컬렉션 목록에서 제외
for table in cursor:
    if table[0] == 'station_near':
        pass
    else:
        table_list.append(table[0])


# 쿼리로 각 테이블 불러와서 df로 만들어 리스트에 저장
df_list = []
for table in table_list:
    sql = f'select * from {table}'
    cursor.execute(query=sql)
    col_name = table.split('_')[0]
    data = []

    for row in cursor:
        row_lst = list(row)
        data.append(row_lst)

    df = pd.DataFrame(data, columns=['station_id', 'station_addr', 'dong_cd', f'{col_name}_cnt'])
    df_list.append(df)
    print(df_list)


# merge 시킬 기준 df 
df_left = df_list[0]

# 반복해서 기준 df(left_df)에 merge
for i in range(1, len(df_list)):
    df_right = df_list[i]
    df_left = pd.merge(df_left, df_right, on=['station_id', 'station_addr', 'dong_cd'], how='inner')


# mysql 저장(db: pjt2)
# table name : station_near
db_connection_str = 'mysql+pymysql://root:1234@localhost/pjt2'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

df_left.to_sql(name='station_near', con=db_connection, if_exists='replace', index=False)