import requests
import json

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
for i in range(0, numResult02) :
    location = r_dict04['SebcCollegeInfoKor']['row'][i]['ADD_KOR']
    url = f"https://dapi.kakao.com/v2/local/search/address.json?query={location}"
    kakao_key = "eeb4d25bd0990160503da341e8678475"
    result = requests.get(url, headers={"Authorization": f"KakaoAK {kakao_key}"})
    json_obj = result.json()
    # x : 경도 / y : 위도
    try :
        x = json_obj['documents'][0]['x']
        y = json_obj['documents'][0]['y']
    except :
        x = 0
        y = 0
    # print(x, y)
    r_dict04['SebcCollegeInfoKor']['row'][i]['위도'] = y
    r_dict04['SebcCollegeInfoKor']['row'][i]['경도'] = x

print(r_dict04['SebcCollegeInfoKor']['row'][30])

# 현재 r_dict04가 x, y 좌표를 붙인 데이터이고, 이것을 json으로 변환하여 하둡에 저장
with open('univ_json.json', 'w') as f:
    json.dump(r_dict04, f, ensure_ascii=False, indent=4)