import requests, json, math

# 데이터 총 갯수
key = '525a415a51766c613132306c5251794f'
startpg = 1
endpg = 1000
url = f'http://openAPI.seoul.go.kr:8088/{key}/json/ListRainfallService/{startpg}/{endpg}/'
resp_cn = requests.get(url)
resp_cn_json = resp_cn.json()
count = int(resp_cn_json['ListRainfallService']['list_total_count'])
count_num = math.trunc(count/1000) + 1

# 데이터 추출
for _ in range(count_num):
    try:
        url = f'http://openAPI.seoul.go.kr:8088/{key}/json/ListRainfallService/{startpg}/{endpg}/'
        startpg = endpg + 1
        endpg += 1000

        response = requests.get(url)
        res_json = response.json()
        stat = res_json['ListRainfallService']['row']
        with open('rainfall.json', 'a', encoding='utf-8') as f:
            json.dump(stat, f, indent=4, sort_keys=True, ensure_ascii=False)
    except:
        break