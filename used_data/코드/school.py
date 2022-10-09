import requests, json, math

# 데이터 총 갯수
key = 'MGlX8jT2jARU535Ywm/oJG192i6N5Bj/xpb8RpxuKOU2o8LihjzxJPC0O0xg6RVZtBL/NvfSaBzhHUJK22CHXQ=='
startpg = 1
url = f'http://api.data.go.kr/openapi/tn_pubr_public_elesch_mskul_lc_api?ServiceKey={key}&numOfRows=1000&pageNo=1&type=json'
resp_cn = requests.get(url)
resp_cn_json = resp_cn.json()
count = int(resp_cn_json['response']['body']['totalCount'])
count_num = math.trunc(count/1000) + 1



# 데이터 추출
for _ in range(count_num):
    try:
        url = f'http://api.data.go.kr/openapi/tn_pubr_public_elesch_mskul_lc_api?ServiceKey={key}&numOfRows=1000&pageNo={startpg}&type=json'
        startpg += 1
        response = requests.get(url)
        res_json = response.json()
        stat = res_json['response']['body']['items']
        with open('school.json', 'a', encoding='utf-8') as f:
            json.dump(stat, f, indent=4, sort_keys=True, ensure_ascii=False)
    except:
        break



