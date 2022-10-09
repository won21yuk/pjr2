import requests
from bs4 import BeautifulSoup as bs
import json

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

