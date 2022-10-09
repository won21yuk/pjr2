import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import json


chrome_options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get('https://culture.seoul.go.kr/culture/culture/cultureEvent/list.do?searchCate=FESTIVAL&menuNo=200010')
lst = list()

for number in range(1, 11):
    time.sleep(5)
    driver.find_element(by=By.CSS_SELECTOR, value=f'#dataList > li:nth-child({number}) > a').send_keys(Keys.ENTER)
    # dataList > li:nth-child(1) > a
    time.sleep(3)
    title = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.event-title > h2').text
    place = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(1) > div.type-td > span').text
    period = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(2) > div.type-td > span').text
    _time = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(3) > div.type-td > span').text
    target = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(4) > div.type-td > span').text
    fee = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(5) > div.type-td > span').text
    dict = {}
    dict['title'] = title
    dict['place'] = place
    dict['period'] = period
    dict['time'] = _time
    dict['target'] = target
    dict['fee'] = fee

    print(f'{title} : {place} / {period} / {_time} / {target} / {fee}')
    lst.append(dict)
    time.sleep(3)
    driver.back()
    time.sleep(3)

for page in range(2, 11):
    for number in range(1, 11):
        time.sleep(5)
        driver.find_element(by=By.CSS_SELECTOR, value=f'#frm > div.event-list-wrap > div.paginationSet > ul > li:nth-child({page}) > a').send_keys(Keys.ENTER)
        time.sleep(3)
        driver.find_element(by=By.CSS_SELECTOR, value=f'#dataList > li:nth-child({number}) > a').send_keys(Keys.ENTER)
        # dataList > li:nth-child(1) > a

        time.sleep(3)
        title = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.event-title > h2').text
        place = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(1) > div.type-td > span').text
        period = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(2) > div.type-td > span').text
        _time = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(3) > div.type-td > span').text
        target = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(4) > div.type-td > span').text
        fee = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(5) > div.type-td > span').text
        dict = {}
        dict['title'] = title
        dict['place'] = place
        dict['period'] = period
        dict['time'] = _time
        dict['target'] = target
        dict['fee'] = fee

        print(f'{title} : {place} / {period} / {_time} / {target} / {fee}')
        lst.append(dict)
        time.sleep(3)
        driver.back()
        time.sleep(3)


time.sleep(10)

for last_page in range(4, 7):
    if last_page < 6:
        for number in range(1, 11):
            time.sleep(5)
            driver.find_element(by=By.CSS_SELECTOR, value='#frm > div.event-list-wrap > div.paginationSet > ul > li.i.end > a').send_keys(Keys.ENTER)

            time.sleep(1)
            driver.find_element(by=By.CSS_SELECTOR, value=f'#frm > div.event-list-wrap > div.paginationSet > ul > li:nth-child({last_page}) > a').send_keys(Keys.ENTER)

            time.sleep(1)
            driver.find_element(by=By.CSS_SELECTOR, value=f'#dataList > li:nth-child({number}) > a').click()
            time.sleep(1)
            title = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.event-title > h2').text
            place = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(1) > div.type-td > span').text
            period = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(2) > div.type-td > span').text
            _time = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(3) > div.type-td > span').text
            target = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(4) > div.type-td > span').text
            fee = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(5) > div.type-td > span').text
            dict = {}
            dict['title'] = title
            dict['place'] = place
            dict['period'] = period
            dict['time'] = _time
            dict['target'] = target
            dict['fee'] = fee

            print(f'{title} : {place} / {period} / {_time} / {target} / {fee}')
            lst.append(dict)
            time.sleep(1)
            driver.back()
            time.sleep(2)
    else:
        for number in range(1, 11):
            try:
                time.sleep(5)
                driver.find_element(by=By.CSS_SELECTOR, value='#frm > div.event-list-wrap > div.paginationSet > ul > li.i.end > a').send_keys(Keys.ENTER)
                time.sleep(1)
                driver.find_element(by=By.CSS_SELECTOR, value=f'#dataList > li:nth-child({number}) > a').click()
                time.sleep(1)
                title = driver.find_element(by=By.CSS_SELECTOR,  value='#print > div.intro-top.clearfix > div.txt-box > div.event-title > h2').text
                place = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(1) > div.type-td > span').text
                period = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(2) > div.type-td > span').text
                _time = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(3) > div.type-td > span').text
                target = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(4) > div.type-td > span').text
                fee = driver.find_element(by=By.CSS_SELECTOR, value='#print > div.intro-top.clearfix > div.txt-box > div.type-box > ul > li:nth-child(5) > div.type-td > span').text
                dict = {}
                dict['title'] = title
                dict['place'] = place
                dict['period'] = period
                dict['time'] = _time
                dict['target'] = target
                dict['fee'] = fee

                print(f'{title} : {place} / {period} / {_time} / {target} / {fee}')
                lst.append(dict)
                time.sleep(1)
                driver.back()
                time.sleep(1)
            except:
                break

res = {'festival': lst}
culture_json = json.dumps(res, ensure_ascii=False, indent=4)
with open('festival.json', 'w', encoding='utf-8') as f:
    f.write(culture_json)

driver.close()