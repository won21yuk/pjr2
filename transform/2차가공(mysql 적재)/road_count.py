from pymongo import MongoClient
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import pymysql


# 전역변수
client = MongoClient('localhost', 27017)
db = client.pjt2
bike_station = db['BIKE_STATION']
cursor = bike_station.find()


# km -> mile 변환함수
def km_to_mile(km):
    mile = km*0.621371
    return float(mile)

# 중심좌표 기준 카운팅 함수
def cnt_doc(collection, distance):
    list = []
    dist = km_to_mile(distance)/3963.2
    coll = db[collection]
    coll_nm = collection.lower()
    for doc in cursor:
        coords = doc['location']['coordinates']
        stations = doc['bike_station_id']
        dongs = doc['dong_cd']
        try:
            addr = doc['station_addr']
        except:
            addr = ""
        if coords != [0.0, 0.0]:
            cnt = coll.count_documents({
                'location': {
                    '$geoWithin': {
                        '$centerSphere': [coords, dist],
                    }
                }
            })
        else:
            cnt = 0
        arr = [stations, addr, dongs, cnt]
        list.append(arr)

    df = pd.DataFrame(list, columns=['station_id', 'station_addr', 'dong_cd', f'{coll_nm}_cnt'])

    return df

# 메인
df_road = cnt_doc('BIKE_ROAD', 2)
print(df_road)


# mysql 저장(db: pjt2)
# table name : road_count
db_connection_str = 'mysql+pymysql://root:1234@localhost/pjt2'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

df_road.to_sql(name='road_count', con=db_connection, if_exists='replace', index=False)