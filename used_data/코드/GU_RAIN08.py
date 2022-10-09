import requests, json, math
from datetime import datetime, timedelta
from pendulum import yesterday

# 함수부
today = datetime.today()
yes = today - timedelta(1)
august = str(yes.strftime("%Y-%m-%d")).split('-')[1]
print(august)


# def _get_gurain():
startpg = 1
endpg = 1000
key = '525a415a51766c613132306c5251794f'
url = f'http://openAPI.seoul.go.kr:8088/{key}/json/ListRainfallService/{startpg}/{endpg}/'
resp_cn = requests.get(url)
resp_cn_json = resp_cn.json()
count = int(resp_cn_json['ListRainfallService']['list_total_count'])
count_num = math.trunc(count/1000) + 1
print(count_num)

gu_rain_list = []
for _ in range(100):
    url = f'http://openAPI.seoul.go.kr:8088/{key}/json/ListRainfallService/{startpg}/{endpg}/'
    startpg = endpg + 1
    endpg += 1000

    response = requests.get(url)
    res_json = response.json()
    stat = res_json['ListRainfallService']['row']
    print(stat)
    #input_dict = json.load(stat)

    filter_date = [x for x in stat if x['RECEIVE_TIME'].split('-')[1] == august]
    # date = yesterday.replace('-', '')
    if not filter_date:
        pass
    else:
        gu_rain_list.extend(filter_date)

with open(f'rain_fall2022{august}.json', 'w', encoding='utf-8') as f:
    json.dump(gu_rain_list, f, indent=4, sort_keys=True, ensure_ascii=False)
