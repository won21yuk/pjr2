import requests, json, math
from datetime import datetime, timedelta
from pendulum import yesterday

serviceKey = '684f6f61546a6f6f33374b6f6c5450'



startpg = 1
endpg = 1000
# 데이터 총 개수
url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/SPOP_LOCAL_RESD_DONG/{startpg}/{endpg}/'
resp_cn = requests.get(url)
resp_cn_json = resp_cn.json()
count = int(resp_cn_json['SPOP_LOCAL_RESD_DONG']['list_total_count'])
count_num = math.trunc(count/1000) + 1
half_count = math.trunc(count_num/2) + 1
life_pop_list = []

for _ in range(half_count):
    url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/SPOP_LOCAL_RESD_DONG/{startpg}/{endpg}/'
    startpg = endpg + 1
    endpg += 1000

    response = requests.get(url)
    res_json = response.json()
    stat = res_json['SPOP_LOCAL_RESD_DONG']['row']
    filter_date = [x for x in stat if x['STDR_DE_ID'][:8] == '202208']
    if not filter_date:
        pass
    else:
        life_pop_list.extend(filter_date)

with open('life_popul20220801.json', 'w', encoding='utf-8') as f:
    json.dump(life_pop_list, f, indent=4, sort_keys=True, ensure_ascii=False)
