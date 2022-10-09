import requests, json, math


serviceKey = '684f6f61546a6f6f33374b6f6c5450'
def university():
    key = '6c70526b4f67773537355357454f4b'
    # 서울시 문화공간 정보 api
    url_munwha = f'http://openapi.seoul.go.kr:8088/{key}/json/culturalSpaceInfo/1/5/'
    # 전체 데이터 개수 확인
    resp01 = requests.get(url_munwha)
    r_dict01 = json.loads(resp01.text)
    # print(r_dict01)
    numOfRows_munwha = r_dict01['culturalSpaceInfo']['list_total_count']

    # 전체 데이터 가져오기
    url02 = f'http://openapi.seoul.go.kr:8088/{key}/json/culturalSpaceInfo/1/{numOfRows_munwha}/'
    resp02 = requests.get(url02)
    r_dict02 = json.loads(resp02.text)
    # 잘 가져왔는지 확인
    # print(r_dict02)
    numResult = len(r_dict02['culturalSpaceInfo']['row'])
    # print(numResult)
    # print(r_dict02['culturalSpaceInfo']['row'][100])

    # 서울시내 대학 및 전문대학 DB 정보
    url_univ = f'http://openapi.seoul.go.kr:8088/{key}/json/SebcCollegeInfoKor/1/5/'
    # 전체 데이터 개수 확인
    resp03 = requests.get(url_univ)
    r_dict03 = json.loads(resp03.text)
    print(r_dict03)
    numOfRows_univ = r_dict03['SebcCollegeInfoKor']['list_total_count']
    print(numOfRows_univ)

    # 전체 데이터 가져오기
    url04 = f'http://openapi.seoul.go.kr:8088/{key}/json/SebcCollegeInfoKor/1/{numOfRows_univ}/'
    resp04 = requests.get(url04)
    r_dict04 = json.loads(resp04.text)
    # 잘 가져왔는지 확인
    print(r_dict04['SebcCollegeInfoKor']['row'][30])
    numResult02 = len(r_dict04['SebcCollegeInfoKor']['row'])
    # print(numResult02)
    # print(r_dict02['culturalSpaceInfo']['row'][100])

    # 대학목록 API에서 주소 가져와서 kakao API에서 X, Y 좌표 뽑아오기
    '''
    location = r_dict04['SebcCollegeInfoKor']['row'][0]['ADD_KOR']
    url = f"https://dapi.kakao.com/v2/local/search/address.json?query={location}"
    kakao_key = "eeb4d25bd0990160503da341e8678475"
    result = requests.get(url, headers={"Authorization":f"KakaoAK {kakao_key}"})
    json_obj = result.json()
    print(json_obj)
    x = json_obj['documents'][0]['x']
    y = json_obj['documents'][0]['y']
    print(x, y)
    '''
    # 데이터 개수만큼 반복하기 - dict에 붙이기
    for i in range(0, numResult02):
        location = r_dict04['SebcCollegeInfoKor']['row'][i]['ADD_KOR']
        url = f"https://dapi.kakao.com/v2/local/search/address.json?query={location}"
        kakao_key = "eeb4d25bd0990160503da341e8678475"
        result = requests.get(url, headers={"Authorization": f"KakaoAK {kakao_key}"})
        json_obj = result.json()
        # x : 경도 / y : 위도
        try:
            x = json_obj['documents'][0]['x']
            y = json_obj['documents'][0]['y']
        except:
            x = 0
            y = 0
        # print(x, y)
        r_dict04['SebcCollegeInfoKor']['row'][i]['위도'] = y
        r_dict04['SebcCollegeInfoKor']['row'][i]['경도'] = x

    print(r_dict04['SebcCollegeInfoKor']['row'][30])

    # 현재 r_dict04가 x, y 좌표를 붙인 데이터이고, 이것을 json으로 변환하여 하둡에 저장
    with open('university.json', 'w') as f:
        json.dump(r_dict04, f, ensure_ascii=False, indent=4)

def mall():
    url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/LOCALDATA_082501/1/1000/'
    response = requests.get(url)
    rjson = response.json()
    stat = rjson['LOCALDATA_082501']
    with open('mall.json', 'w') as f:
        json.dump(stat, f)



def lifepopulation():
    startpg = 1
    endpg = 1000
    # 데이터 총 개수
    url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/SPOP_LOCAL_RESD_DONG/{startpg}/{endpg}/'
    resp_cn = requests.get(url)
    resp_cn_json = resp_cn.json()
    count = int(resp_cn_json['ListRainfallService']['list_total_count'])
    count_num = math.trunc(count/1000) + 1
    for _ in range(count_num):
        url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/SPOP_LOCAL_RESD_DONG/{startpg}/{endpg}/'
        response = requests.get(url)
        rjson = response.json()
        stat = rjson['SPOP_LOCAL_RESD_DONG']
        startpg=endpg+1
        endpg=endpg+1000
        with open('life_popul.json', 'a') as f:
            json.dump(stat, f)

# date = 20220701
# startpg = 1
# endpg = 1000
# serviceKey = '786f67664c6a6f6f3636665a727a68'
# url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/SPOP_LOCAL_RESD_DONG/{startpg}/{endpg}/{date}'
# response = requests.get(url)
# rjson = response.json()
# print(rjson)

def subwayinfo():
    url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/subwayStationMaster/1/1000/'
    response = requests.get(url)
    rjson = response.json()
    stat = rjson['subwayStationMaster']
    with open('subwayinfo.json', 'w') as f:
        json.dump(stat, f)

def businfo():
    startpg = 1
    endpg = 1000
    # 데이터 총 개수
    url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/tbisMasterStation/{startpg}/{endpg}/'
    resp_cn = requests.get(url)
    resp_cn_json = resp_cn.json()
    count = int(resp_cn_json['ListRainfallService']['list_total_count'])
    count_num = math.trunc(count/1000) + 1
    for _ in range(count_num):
        url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/tbisMasterStation/{startpg}/{endpg}/'
        startpg = endpg + 1
        endpg = endpg + 1000
        response = requests.get(url)
        rjson = response.json()
        stat = rjson['tbisMasterStation']
        with open('businfo.json', 'a') as f:
            json.dump(stat, f)

def park():
    url = f'http://openAPI.seoul.go.kr:8088/{serviceKey}/json/SearchParkInfoService/1/132/'
    response = requests.get(url)
    rjson = response.json()
    stat = rjson['SearchParkInfoService']
    with open('park.json', 'w') as f:
        json.dump(stat, f)

def buscountinfo():
    startpg = 1
    endpg = 1000
    # 데이터 총 개수
    url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/tpssEmdBus/{startpg}/{endpg}/'
    resp_cn = requests.get(url)
    resp_cn_json = resp_cn.json()
    count = int(resp_cn_json['ListRainfallService']['list_total_count'])
    count_num = math.trunc(count/1000) + 1
    for _ in range(count_num):
        url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/tpssEmdBus/{startpg}/{endpg}/'
        startpg = endpg + 1
        endpg = endpg + 1000
        response = requests.get(url)
        rjson = response.json()
        stat = rjson['tpssEmdBus']
        with open('buscountinfo.json', 'a') as f:
            json.dump(stat, f)



def subwaycountinfo():
    startpg = 1
    endpg = 1000
    # 데이터 총 개수
    url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/tpssSubwayPassenger/{startpg}/{endpg}/'
    resp_cn = requests.get(url)
    resp_cn_json = resp_cn.json()
    count = int(resp_cn_json['ListRainfallService']['list_total_count'])
    count_num = math.trunc(count/1000) + 1
    for _ in range(count_num):
        url = f'http://openapi.seoul.go.kr:8088/{serviceKey}/json/tpssSubwayPassenger/{startpg}/{endpg}/'
        startpg = endpg + 1
        endpg = endpg + 1000
        response = requests.get(url)
        rjson = response.json()
        stat = rjson['tpssSubwayPassenger']
        with open('subwaycountinfo.json', 'a') as f:
            json.dump(stat, f)

def school():
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

def rainfall():
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

def bike():
    for i in [19000001, 19000002, 19000005, 19000006, 19000007, 19000008, 19000010, 19000011, 19000013, 19000014, 17001211]:
        url = f'https://map.naver.com/v5/api/aoi?type_name=LINK&wsid=1.0&crs=epsg:4326&FID={i}'
        headers = {
            'Referer': 'https://map.naver.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        bike_json = response.json()

        with open(f'bike_{i}.json', 'w', encoding='utf-8') as f:
            json.dump(bike_json, f, indent=4, sort_keys=True, ensure_ascii=False)



