import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "seoul_bike.settings")
import django
django.setup()

import plotly.express as px
import plotly.graph_objects as go
from seoul_bike.models import *
import pandas as pd

import plotly.figure_factory as ff
from plotly.offline import plot
from plotly.subplots import make_subplots

from pymongo import MongoClient

#######  [ 데이터 탐색 ]  #######
# 따릉이 대여소 갯수 (station_near 기준)
def countStationId():
    df = pd.DataFrame(list(StationNear.objects.all().values('station_id')))
    result = df['station_id'].count()
    return result


# 1년간 이용건수
def year_usage():
    month_usage = pd.DataFrame(list(MonthUsage.objects.all().values()))
    result = month_usage['usage_amt'].sum()

    return result


# 따릉이 대여소 top3
def topStation_id():
    df = pd.DataFrame(list(StationUsage.objects.all().values('station_id', 'rent_amt', 'return_amt')))
    df2 = df.groupby(by=['station_id']).sum().reset_index()
    df2 = df2.sort_values(['rent_amt', 'return_amt'], ascending=False)
    result = dict()
    top1 = df2.iloc[0:1]
    top2 = df2.iloc[1:2]
    top3 = df2.iloc[2:3]
    result['top1_station_id'] = top1['station_id'].values[0]
    result['top1_rent_amt'] = top1['rent_amt'].values[0]
    result['top1_return_amt'] = top1['return_amt'].values[0]
    result['top2_station_id'] = top2['station_id'].values[0]
    result['top2_rent_amt'] = top2['rent_amt'].values[0]
    result['top2_return_amt'] = top2['return_amt'].values[0]
    result['top3_station_id'] = top3['station_id'].values[0]
    result['top3_rent_amt'] = top3['rent_amt'].values[0]
    result['top3_return_amt'] = top3['return_amt'].values[0]
    return result


# 강우량과 공공자전거 이용량
def rain_usage():
    rain_usage06 = pd.DataFrame(list(RainUsage06.objects.all().values()))

    # rain_amt 내림차순으로 sort 한 후 처음 0인 지점까지 가져온다
    # x : 강우량 y : 이용량
    rain_usage06 = rain_usage06.sort_values('rain_amt', ascending=False)
    rain_usage06 = rain_usage06.reset_index(drop=True)
    index = 0
    while(True):
        if(rain_usage06['rain_amt'][index] == 0):
            break
        else : index += 1
    rainusage = rain_usage06[0:index+1]
    rainusage = rainusage.sort_values('rain_amt')
    rainusage = rainusage.astype({'rain_amt':'string'})

    fig = px.bar(rainusage, x='rain_amt', y='usage_amt', color_discrete_sequence =['mediumturquoise'])
    fig.update_traces(hovertemplate='<b>%{text}</b><br>', text=rainusage.usage_amt)
    fig.update_coloraxes(showscale=False)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="강우량 (mm)")
    fig.update_yaxes(title_text="따릉이 이용량 (건)")
    plot_div = plot(fig, output_type='div')

    return plot_div



#######  [ 시간적 요소 ]  #######
# 윌/시간대 - 월별 따릉이 이용량
def monthusage():
    monthusage = pd.DataFrame(list(MonthUsage.objects.all().values()))
    fig = px.bar(monthusage, x= 'base_mm', y= 'usage_amt',
                 color_discrete_sequence =['darkcyan'])
    fig.update_traces(hovertemplate='<b>%{text}</b><br>', text=monthusage.usage_amt)
    fig.update_coloraxes(showscale=False)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="연월")
    fig.update_yaxes(title_text="따릉이 이용량 (건)")
    plot_div = plot(fig, output_type='div')
    return plot_div

# 윌/시간대 - 시간대별 따릉이 이용량
def timeusage():
    timeusage = pd.DataFrame(list(TimeUsage.objects.all().values()))
    fig = go.Figure()
    # fig = px.line(timeusage, x='base_tm', y='usage_amt', template='plotly_white')
    fig.add_trace(go.Scatter(x=timeusage.base_tm, y=timeusage.usage_amt, mode='lines',
                             line={'width': 5}, fill='tozeroy', fillcolor='rgba(65, 105, 225, 0.3)'))
    fig.update_coloraxes(showscale=False)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="시간대")
    fig.update_yaxes(title_text="따릉이 이용량 (건)")
    plot_div = plot(fig, output_type='div')
    return plot_div


# 생활인구/유동인구 - 시간대별 생활인구와 따릉이 이용량
def lifeusage():
    populusage = pd.DataFrame(list(PopulUsage.objects.all().values()))
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=populusage.base_tm, y=populusage['life_popul'], mode='lines+markers',
                             line={'width': 5}, marker=dict(color='gold', size=10)), secondary_y=True)
    fig.add_trace(go.Bar(x=populusage.base_tm, y=populusage['usage_amt'], name="사용량",
                        marker=dict(color=populusage['usage_amt'], colorscale='tealgrn')), secondary_y=False)
    fig.update_layout(template='plotly_white', showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="시간대별 생활인구와 따릉이 이용량", tickangle=45)
    fig.update_yaxes(title_text="생활인구", secondary_y=True)
    fig.update_yaxes(title_text="따릉이 이용량 (건)", secondary_y=False)
    plot_div = plot(fig, output_type='div')
    return plot_div


# 생활인구/유동인구 - 시간대별 지하철 유동인구와 따릉이 이용량
def subusage():
    populusage = pd.DataFrame(list(PopulUsage.objects.all().values()))
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=populusage.base_tm, y=populusage['sub_popul'], mode='lines+markers',
                            line={'width': 5}, marker=dict(color='gold', size=10)), secondary_y=True)
    fig.add_trace(go.Bar(x=populusage.base_tm, y=populusage['usage_amt'], name="사용량",
                        marker=dict(color=populusage['usage_amt'], colorscale='tealgrn')), secondary_y=False)
    fig.update_layout(template='plotly_white', showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="시간대별 지하철 유동인구와 따릉이 이용량", tickangle=45)
    fig.update_yaxes(title_text="지하철 유동인구", secondary_y=True)
    fig.update_yaxes(title_text="따릉이 이용량 (건)", secondary_y=False)
    plot_div = plot(fig, output_type='div')
    return plot_div


# 생활인구/유동인구 - 시간대별 버스 유동인구와 따릉이 이용량
def bususage():
    populusage = pd.DataFrame(list(PopulUsage.objects.all().values()))
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=populusage.base_tm, y=populusage['bus_popul'], mode='lines+markers',
                            line={'width': 5}, marker=dict(color='gold', size=10)), secondary_y=True)
    fig.add_trace(go.Bar(x=populusage.base_tm, y=populusage['usage_amt'], name="사용량",
                        marker=dict(color=populusage['usage_amt'], colorscale='tealgrn')), secondary_y=False)
    fig.update_layout(template='plotly_white', showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="시간대별 버스 유동인구와 따릉이 이용량", tickangle=45)
    fig.update_yaxes(title_text="버스 유동인구", secondary_y=True)
    fig.update_yaxes(title_text="따릉이 이용량 (건)", secondary_y=False)
    plot_div = plot(fig, output_type='div')
    return plot_div


#######  [ 공간적 요소 ]  #######
# 대여소별 교통시설 개수와 이용량
def transportation():
    station_near = pd.DataFrame(list(StationNear.objects.all().values()))
    station_near['transportation'] = station_near['bus_cnt'] + station_near['sub_cnt']
    station_usage = pd.DataFrame(list(StationUsage.objects.all().values()))
    station_near = station_near[['station_id', 'transportation']]
    station_usage['usage_amt'] = station_usage['rent_amt'] + station_usage['return_amt']
    station_usage = station_usage[['station_id', 'usage_amt']]
    merged = station_usage.merge(station_near, how='inner', on='station_id')

    fig = px.scatter(merged, x='transportation', y='usage_amt', color='transportation', size='transportation',
                    color_continuous_scale='tealgrn', template='plotly_white')
    fig.update_traces(hovertemplate='<b>%{text}</b><br>교통시설: %{x:.0f}개<br>이용량: %{y:.0f}건', text=merged.station_id)
    fig.update_coloraxes(showscale=False)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="대여소 근처 교통시설 수")
    fig.update_yaxes(title_text="대여소 이용량 (건)")
    fig.update_yaxes(range=[0, 200000])
    plot_div = plot(fig, output_type='div')
    return plot_div

# 대여소별 근린시설 개수와 이용량
def neighborhood():
    station_near = pd.DataFrame(list(StationNear.objects.all().values()))
    station_near['neighborhood'] = station_near['culture_cnt'] + station_near['mall_cnt'] + station_near['park_cnt'] + station_near['event_cnt'] + station_near['tour_cnt']
    station_usage = pd.DataFrame(list(StationUsage.objects.all().values()))
    station_near = station_near[['station_id', 'neighborhood']]
    station_usage['usage_amt'] = station_usage['rent_amt'] + station_usage['return_amt']
    station_usage = station_usage[['station_id', 'usage_amt']]
    merged = station_usage.merge(station_near, how='inner', on='station_id')

    fig = px.scatter(merged, x='neighborhood', y='usage_amt', color='neighborhood', size='neighborhood',
                    color_continuous_scale='tealrose', template='plotly_white')
    fig.update_traces(hovertemplate='<b>%{text}</b><br>근린시설: %{x:.0f}개<br>이용량: %{y:.0f}건', text=merged.station_id)
    fig.update_coloraxes(showscale=False)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="대여소 근처 근린시설 수")
    fig.update_yaxes(title_text="대여소 이용량 (건)")
    fig.update_yaxes(range=[0, 200000])
    plot_div = plot(fig, output_type='div')
    return plot_div

# 대여소별 교육시설 개수와 이용량
def education():
    station_near = pd.DataFrame(list(StationNear.objects.all().values()))
    station_usage = pd.DataFrame(list(StationUsage.objects.all().values()))
    station_near = station_near[['station_id', 'school_cnt']]
    station_usage['usage_amt'] = station_usage['rent_amt'] + station_usage['return_amt']
    station_usage = station_usage[['station_id', 'usage_amt']]
    merged = station_usage.merge(station_near, how='inner', on='station_id')

    fig = px.scatter(merged, x='school_cnt', y='usage_amt', color='school_cnt', size='school_cnt',
                    color_continuous_scale='algae', template='plotly_white')
    fig.update_traces(hovertemplate='<b>%{text}</b><br>교육시설: %{x:.0f}개<br>이용량: %{y:.0f}건', text=merged.station_id)
    fig.update_coloraxes(showscale=False)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(title_text="대여소 근처 교육시설 수")
    fig.update_yaxes(title_text="대여소 이용량 (건)")
    fig.update_yaxes(range=[0, 200000])
    plot_div = plot(fig, output_type='div')
    return plot_div


## 지도 그리기
# km -> mile 변환
def km_to_mile(km):
    mile = km * 0.621371
    return float(mile)


# coords = [lon, lat]
def _get_map(collection, coords, distance):
    client = MongoClient("mongodb://team01:1234@54.248.183.216", 27017)
    db = client['pjt2']
    coll = db[collection]
    dist = km_to_mile(distance) / 3963.2
    collection_list = []
    cursor = coll.find({
        'location': {
            '$geoWithin': {
                '$centerSphere': [coords, dist]
            }
        }
    }, {'_id': 0})
    for doc in cursor:
        # if len(doc['location']['coordinates'][0])
        doc['lon'] = doc['location']['coordinates'][0]
        doc['lat'] = doc['location']['coordinates'][1]
        del doc['location']
        collection_list.append(doc)
    df = pd.DataFrame(collection_list)

    return df


def ranking():
    client = MongoClient("mongodb://team01:1234@54.248.183.216", 27017)
    db = client['pjt2']
    bike_station = db['BIKE_STATION']
    id_list = ['207', '502', '2715']
    info_list = []
    for id in id_list:
        info = bike_station.find({'bike_station_id': f'{id}'}, {'_id': 0})[0]
        info['coords'] = [info['location']['coordinates'][0], info['location']['coordinates'][1]]
        info['lon'] = info['location']['coordinates'][0]
        info['lat'] = info['location']['coordinates'][1]
        del info['location']
        info_list.append(info)

    return info_list


def transportation_facility():
    # 교통시설 : BUS_STATION / SUBWAY_STATION / BIKE_ROAD
    rank_list = ranking()
    center = [rank_list[0]['coords'], rank_list[1]['coords'], rank_list[2]['coords']]
    mapboxt = open("seoul_bike/mapbox_token.py").read().rstrip()

    # 1번 df
    df_first = pd.DataFrame(rank_list[0])
    df_bus1 = _get_map('BUS_STATION', center[0], 2)
    df_sub1 = _get_map('SUBWAY_STATION', center[0], 2)
    # df_road1 = _get_map('BIKE_ROAD', center[0], 2)

    # 2번 df
    df_second = pd.DataFrame(rank_list[1])
    df_bus2 = _get_map('BUS_STATION', center[1], 2)
    df_sub2 = _get_map('SUBWAY_STATION', center[1], 2)
    # df_road2 = _get_map('BIKE_ROAD', center[1], 2)

    # 3번 df
    df_third = pd.DataFrame(rank_list[2])
    df_bus3 = _get_map('BUS_STATION', center[2], 2)
    df_sub3 = _get_map('SUBWAY_STATION', center[2], 2)
    # df_road3 = _get_map('BIKE_ROAD', center[2], 2)

    # 1등 대여소
    fig_first = px.scatter_mapbox(df_bus1, lat='lat', lon='lon', hover_name='bus_station_name', color_discrete_sequence=['turquoise'])
    fig_first.add_trace(px.scatter_mapbox(df_sub1, lat='lat', lon='lon', hover_name='subway_station_name', color_discrete_sequence=['teal']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_first, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])
    fig_first.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_first.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_first.update_traces(marker={'size': 10})
    fig_first.update_layout(mapbox_center=dict(lat=center[0][1], lon=center[0][0]))

    # 2등 대여소
    fig_second = px.scatter_mapbox(df_bus2, lat='lat', lon='lon', hover_name='bus_station_name', color_discrete_sequence=['turquoise'])
    fig_second.add_trace(px.scatter_mapbox(df_sub2, lat='lat', lon='lon', hover_name='subway_station_name', color_discrete_sequence=['teal']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_second, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])
    fig_second.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_second.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_second.update_traces(marker={'size': 10})
    fig_second.update_layout(mapbox_center=dict(lat=center[1][1], lon=center[1][0]))
    # 3등 대여소
    fig_third = px.scatter_mapbox(df_bus3, lat='lat', lon='lon', hover_name='bus_station_name', color_discrete_sequence=['turquoise'])
    fig_third.add_trace(px.scatter_mapbox(df_sub3, lat='lat', lon='lon', hover_name='subway_station_name', color_discrete_sequence=['teal']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_third, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])
    fig_third.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_third.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_third.update_traces(marker={'size': 10})
    fig_third.update_layout(mapbox_center=dict(lat=center[2][1], lon=center[2][0]))

    plot_div1 = plot({'data': fig_first}, output_type='div')
    plot_div2 = plot({'data': fig_second}, output_type='div')
    plot_div3 = plot({'data': fig_third}, output_type='div')

    result = {'TF_mymap1': plot_div1, 'TF_mymap2': plot_div2, 'TF_mymap3': plot_div3}

    return result

def neighborhood_facility():
    # 근린시설 : PARK / MALL
    rank_list = ranking()
    center = [rank_list[0]['coords'], rank_list[1]['coords'], rank_list[2]['coords']]
    mapboxt = open("seoul_bike/mapbox_token.py").read().rstrip()

    # 1번 df
    df_first = pd.DataFrame(rank_list[0])
    df_park1 = _get_map('PARK', center[0], 2)
    df_mall1 = _get_map('MALL', center[0], 2)

    # 2번 df
    df_second = pd.DataFrame(rank_list[1])
    df_park2 = _get_map('PARK', center[1], 2)
    df_mall2 = _get_map('MALL', center[1], 2)

    # 3번 df
    df_third = pd.DataFrame(rank_list[2])
    df_park3 = _get_map('PARK', center[2], 2)
    df_mall3 = _get_map('MALL', center[2], 2)

    # 1등 대여소
    fig_first = px.scatter_mapbox(df_park1, lat='lat', lon='lon', hover_name='park_nm', color_discrete_sequence=['seagreen'])
    fig_first.add_trace(px.scatter_mapbox(df_mall1, lat='lat', lon='lon', hover_name='mall_nm', color_discrete_sequence=['mediumaquamarine']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_first, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])
    fig_first.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_first.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_first.update_traces(marker={'size': 10})
    fig_first.update_layout(mapbox_center=dict(lat=center[0][1], lon=center[0][0]))

    # 2등 대여소
    fig_second = px.scatter_mapbox(df_park2, lat='lat', lon='lon', hover_name='park_nm', color_discrete_sequence=['seagreen'])
    fig_second.add_trace(px.scatter_mapbox(df_mall2, lat='lat', lon='lon', hover_name='mall_nm', color_discrete_sequence=['mediumaquamarine']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_second, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])
    fig_second.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_second.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_second.update_traces(marker={'size': 10})
    fig_second.update_layout(mapbox_center=dict(lat=center[1][1], lon=center[1][0]))
    # 3등 대여소
    fig_third = px.scatter_mapbox(df_park3, lat='lat', lon='lon', hover_name='park_nm', color_discrete_sequence=['seagreen'])
    fig_third.add_trace(px.scatter_mapbox(df_mall3, lat='lat', lon='lon', hover_name='mall_nm', color_discrete_sequence=['mediumaquamarine']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_third, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])
    fig_third.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_third.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_third.update_traces(marker={'size': 10})
    fig_third.update_layout(mapbox_center=dict(lat=center[2][1], lon=center[2][0]))

    plot_div1 = plot({'data': fig_first}, output_type='div')
    plot_div2 = plot({'data': fig_second}, output_type='div')
    plot_div3 = plot({'data': fig_third}, output_type='div')

    result = {'NF_mymap1': plot_div1, 'NF_mymap2': plot_div2, 'NF_mymap3': plot_div3}

    return result

def education_facility():
    # 교육/문화 : SCHOOL / TOUR_PLACE / CULTURE_PLACE / EVENT_PLACE
    rank_list = ranking()
    center = [rank_list[0]['coords'], rank_list[1]['coords'], rank_list[2]['coords']]
    mapboxt = open("seoul_bike/mapbox_token.py").read().rstrip()

    # 1번 df
    df_first = pd.DataFrame(rank_list[0])
    df_school1 = _get_map('SCHOOL', center[0], 2)
    df_tour1 = _get_map('TOUR_PLACE', center[0], 2)
    df_culture1 = _get_map('CULTURE_PLACE', center[0], 2)
    df_event1 = _get_map('EVENT_PLACE', center[0], 2)

    # 2번 df
    df_second = pd.DataFrame(rank_list[1])
    df_school2 = _get_map('SCHOOL', center[1], 2)
    df_tour2 = _get_map('TOUR_PLACE', center[1], 2)
    df_culture2 = _get_map('CULTURE_PLACE', center[1], 2)
    df_event2 = _get_map('EVENT_PLACE', center[1], 2)

    # 3번 df
    df_third = pd.DataFrame(rank_list[2])
    df_school3 = _get_map('SCHOOL', center[2], 2)
    df_tour3 = _get_map('TOUR_PLACE', center[2], 2)
    df_culture3 = _get_map('CULTURE_PLACE', center[2], 2)
    df_event3 = _get_map('EVENT_PLACE', center[2], 2)

    # 1등 대여소
    fig_first = px.scatter_mapbox(df_school1, lat='lat', lon='lon', hover_name='school_nm', color_discrete_sequence=['lime'])
    fig_first.add_trace(px.scatter_mapbox(df_tour1, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['greenyellow']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_culture1, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['mediumseagreen']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_event1, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['darkgreen']).data[0])
    fig_first.add_trace(px.scatter_mapbox(df_first, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])

    fig_first.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_first.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_first.update_traces(marker={'size': 10})
    fig_first.update_layout(mapbox_center=dict(lat=center[0][1], lon=center[0][0]))

    # 2등 대여소
    fig_second = px.scatter_mapbox(df_school2, lat='lat', lon='lon', hover_name='school_nm', color_discrete_sequence=['lime'])
    fig_second.add_trace(px.scatter_mapbox(df_tour2, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['greenyellow']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_culture2, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['mediumseagreen']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_event2, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['darkgreen']).data[0])
    fig_second.add_trace(px.scatter_mapbox(df_second, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])

    fig_second.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_second.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_second.update_traces(marker={'size': 10})
    fig_second.update_layout(mapbox_center=dict(lat=center[1][1], lon=center[1][0]))

    # 3등 대여소
    fig_third = px.scatter_mapbox(df_school3, lat='lat', lon='lon', hover_name='school_nm', color_discrete_sequence=['lime'])
    fig_third.add_trace(px.scatter_mapbox(df_tour3, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['greenyellow']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_culture3, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['mediumseagreen']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_event3, lat='lat', lon='lon', hover_name='place_nm', color_discrete_sequence=['darkgreen']).data[0])
    fig_third.add_trace(px.scatter_mapbox(df_third, lat="lat", lon="lon", hover_data=['bike_station_id', 'station_addr'], color_discrete_sequence=['red']).data[0])

    fig_third.update_layout(autosize=True, hovermode='closest', margin=dict(l=0, r=0, b=0, t=0))
    fig_third.update_mapboxes(bearing=0, accesstoken=mapboxt, pitch=0, zoom=12)
    fig_third.update_traces(marker={'size': 10})
    fig_third.update_layout(mapbox_center=dict(lat=center[2][1], lon=center[2][0]))

    plot_div1 = plot({'data': fig_first}, output_type='div')
    plot_div2 = plot({'data': fig_second}, output_type='div')
    plot_div3 = plot({'data': fig_third}, output_type='div')

    result = {'EF_mymap1': plot_div1, 'EF_mymap2': plot_div2, 'EF_mymap3': plot_div3}

    return result
