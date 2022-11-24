<div align=center>
    <h1>데이터 파이프라인 구축 프로젝트</h1>
</div>

>대여소별 시공간적 특성을 활용한 따릉이 이용량 예측 및 재배치를 위한 데이터 파이프라인 구축
> 

본 프로젝트는 대여소 별 시공간적 특성에 따른 수요 예측을 통한 따릉이 자전거의 재배치를 목표로 합니다. 데이터 분석가가 따릉이 수요 예측에 사용할 수 있는 데이터를 제공하기 위한 데이터 파이프라인을 구축하였고 이를 통해 만들어진 데이터를 대시보드로 만들어 시각화 하였습니다.

<div align=left>
    <br>
    <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white">
    <img src="https://img.shields.io/badge/django-092E20?style=for-the-badge&logo=django&logoColor=white">
    <img src="https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=Apache%20Hadoop&logoColor=white">
    <img src="https://img.shields.io/badge/Apache Spark-E25A1C?style=for-the-badge&logo=Apache Spark&logoColor=white">
    <img src="https://img.shields.io/badge/Apache Airflow-017CEE?style=for-the-badge&logo=Apache Airflow&logoColor=white">
    <img src="https://img.shields.io/badge/MongoDB-47A2448?style=for-the-badge&logo=MongoDB&logoColor=white">
    <img src="https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=MySQL&logoColor=white">
    <img src="https://img.shields.io/badge/Amazon EC2-FF9900?style=for-the-badge&logo=Amazon EC2&logoColor=white">
</div>
<br>

프로젝트 기간 : 2022.07.25 ~ 2022.08.17


# 프로젝트 배경

- 서울시 따릉이는 2021년 기준 누적 이용건이 1억건을 돌파할 정도로 대중적인 교통수단으로 자리잡음.
- 그러나 공공 자전거 사업 손실은 점차 증가추세(2016년 25억 적자 -> 2021년 103억 적자)
- 이러한 적자는 높은 재배치 비용 때문(전체 운영비의 1/3이 재배치 예산)
- 재배치 비용이 높은 건 현재 서울시의 재배치 시스템이 현장대응형으로서 재배치 요원의 직관적 판단에 의존하기 때문.
- 따라서 제한된 자원으로 전체 시스템을 효율적으로 운영하기 위해서 자전거 수요량을 예측하고 이를 토대로 선제적인 재배치가 이루어져야함.

# 변수 선정

- 따릉이 이용량 예측에 필요한 변수를 시간적 변수/공간적 변수로 구분
    - 시간적 변수
        - 일별, 월별, 휴일 여부 등이 포함된 데이터
    - 공간적 변수
        - 위치정보 데이터(주소, 좌표값)가 있는 데이터
        - 교통시설, 교육시설, 근린시설로 카테고리 구분
- 이상치 처리
    - 강우량은 이용량에 부정적인 영향을 미침
        - 날씨가 자전거 이용에 미치는 영향 분석(2012, 김동준,신희철 외 2명) : ‘강수량은 자전거에 부정적인 영향을 주는데, 강수량이 10cm 증가할 때마다 자전거 이용은 약 60% 감소하는 것으로 나타났다.’
    - 분석의 편의를 위해 강우량이 있었던 일자/시간대는 이상치로서 제거

# 아키텍쳐
![pjt2-arch](/used_data/pjt2-arch.png)

# 세부 구현 내용
- 데이터 처리 및 적재
  - 모든 처리와 DB적재는 pyspark 파일로 작성하여 spark-submit으로 실행
  - 위치 데이터 
    - 1차 가공 : MongoDB 적재
        - 모든 위치 데이터는 geometry 쿼리 사용을 위해 GEOJSON 형태로 처리 및 가공
        - 총 10개의 데이터 가공 및 적재 : 대여소, 자전거 도로, 지하철역, 버스정류장, 학교, 공원, 대규모 점포, 문화공간, 관광명소, 행사장소
    - 2차 가공 : MySQL 적재
        - 대여소별 2KM 반경 내 자전거 도로, 지하철역, 버스정류장, 학교, 공원, 대규모 점포, 문화공간, 관광명소, 행사장소 수를 집계하기 위함
        - 2KM로 선정한 근거 : 네트워크 분석을 통한 공공자전거 따릉이의 이용특성에 대한 연구 - ‘다른 대여소에 반납할 때 70%는 10분 이하, 2km 이내의 짧은 거리를 이동하였으며, 같은 대여소에 반납할 때에는 30분 이상 대여하는 경우가 많았고 이용 시간보다 비교적 짧은 거리인 2~4km를 이동했다.’
  - 위치 데이터 외 데이터
    - 가공 및 MySQL 적재

- 파이프 라인 운영
    - DAG 1. DONG_POPUL
        - 행정동 단위 서울 생활 인구
            - API로 가져올 수 있는 데이터 최근 2개월까지
            - 1시간 단위 데이터이며 일자별로 업데이트
            - 현재시점 기준 5일전 자료가 최신
        - 서울시 행정동별 버스 총 승차 승객수 정보
            - API로 가져올 수 있는 데이터 최근 2개월까지
            - 1시간 단위 데이터이며 일자별로 업데이트
            - 현재시점 기준 5일전 자료가 최신
        - 서울시 행정동별 지하철 총 승차 승객수 정보
            - API로 가져올 수 있는 데이터 최근 2개월까지
            - 1시간 단위 데이터이며 일자별로 업데이트
            - 현재시점 5일전 자료가 최신
        - Schedule_interval
            - Daily
            - 데이터가 업데이트 되는 시간대가 제각각이기 때문에 다음날 자정에 6일전 데이터를 동시에 가져오는 방식으로 구현
            - 이는 각 데이터의 업데이트 주기를 통일하기 위함
    - DAG2. GU_RAIN
        - 서울시 강우량 정보
            - API로 가져올 수 있는 데이터 최근 1개월까지
            - 10분 단위 데이터
            - 매 9분 마다(9, 19, 29, 39, 49, 59) 업데이트
        - Schedule_interval
            - Daily
            - 데이터가 매 9분마다 갱신되기 때문에 매일 자정에 DAG를 실행시켜서 그 전날의 모든 강수량 정보 데이터를 가져오도록 설정
    - DAG3. DELETE
        - AWS의 홈디렉토리와 HDFS에 적재된 자료들을 주기적으로 정리하기 위한 DAG파일
        - MONTHLY
    - DAG 구분 기준 : MySQL에 적재되는 테이블을 기준으로 작성(DONG_POPUL, GU_RAIN)
        - 테이블 선정 기준 : 적어도 일일 단위로 업데이트되는 시계열 배치 데이터들을 최신정보로 분석 데이터셋에 반영하기 위함
    - DAG들의 DEPENDECY 설정
        - Raw data 수집 및 aws 홈디렉토리에 저장(python operator) -> HDFS로 적재(bash operator) -> 가공 및 DB적재(spark-submit operator)

# 사용 데이터

| no | 이름 | 출처 | 형식/방식 |
| --- | --- | --- | --- |
| 1 | 서울시 따릉이 대여소 마스터 정보 | 서울 열린데이터 광장 | CSV/파일 |
| 2 | 서울시 공공자전거 실시간 대여정보 | 서울 열린데이터 광장 | JSON/API |
| 3 | 서울시 공공자전거 대여이력 정보 | 서울 열린데이터 광장 | CSV/파일 |
| 4 | 서울시 행정구역 코드 정보 | 서울 열린데이터 광장 | CSV/파일 |
| 5 | 행정동 단위 서울 생활인구(내국인) | 서울 열린데이터 광장 | JSON/API |
| 6 | 서울시 행정동별 버스 총 승차 승객수 정보 | 서울 열린데이터 광장 | JSON/API |
| 7 | 서울시 행정동별 지하철 총 승차 승객수 정보 | 서울 열린데이터 광장 | JSON/API |
| 8 | 서울시 역사마스터 정보 | 서울 열린데이터 광장 | JSON/API |
| 9 | 서울시 정류장마스터 정보 | 서울 열린데이터 광장 | JSON/API |
| 10 | 서울시 자전거 도로 | 네이버 지도 | JSON/API |
| 11 | 전국초중등학교 위치표준데이터 | 공공데이터포털 | CSV/파일 |
| 12 | 서울시 대학 및 전문대학 DB 정보(한국어) | 서울 열린데이터 광장 | JSON/API |
| 13 | 서울시 대학 위치데이터 | 카카오 지도 | JSON/API |
| 14 | 서울시 대규모점포 인허가 정보 | 서울 열린데이터 광장 | JSON/API |
| 15 | 서울시 주요 공원 현황 | 서울 열린데이터 광장 | JSON/API |
| 16 | 서울시 문화공간 정보 | 서울 열린데이터 광장 | JSON/API |
| 17 | 서울시 문화행사 정보 | 서울문화포털 | 크롤링, API |
| 18 | 서울시 강우량 정보 | 서울 열린데이터 광장 | JSON, CSV/API, 파일 |
| 19 | 서울시 관광 명소 | 서울시 공식 관광정보 웹사이트 | CSV/크롤링 |
| 20 | 서울시 관광 명소 위치데이터 | 카카오 지도 | JSON/API |
| 21 | 서울시 관광 명소 별점 | 구글맵 | CSV/크롤링 |

# ERD
![pjt2-erd](/used_data/pjt2-erd.png)

# Reference
- 참고 기사
    - [https://www.motorgraph.com/news/articleView.html?idxno=29857](https://www.motorgraph.com/news/articleView.html?idxno=29857)
    - [https://www.mk.co.kr/news/society/view/2022/02/99087/](https://www.mk.co.kr/news/society/view/2022/02/99087/)
    - [https://seoulsolution.kr/ko/content/9613](https://seoulsolution.kr/ko/content/9613)
    - [https://news.mt.co.kr/mtview.php?no=2022061514490583946&VNC_T](https://news.mt.co.kr/mtview.php?no=2022061514490583946&VNC_T)
    - [http://www.sobilife.com/news/articleView.html?idxno=31996](http://www.sobilife.com/news/articleView.html?idxno=31996)
    - [https://www.hankookilbo.com/News/Read/A2021092317270001174?did=MN](https://www.hankookilbo.com/News/Read/A2021092317270001174?did=MN)

- 참고 논문
    - 공공자전거 이용환경 만족도 영향 요인 분석(김소윤, 이경환, 고은정)
    - 서울시 공공자전거 이용에 영향을 미치는 물리적 환경 요인 분석(사경은, 이수기)
    - 서울 공공자전거 신규대여소를 위한 수요량 예측 분석(김예술, 박시온, 박군웅)
    - 공유자전거 시스템의 이용 예측을 위한 K-Means 기반의 군집 알고리즘 (김경옥, 이창환)
    - 서울시 공공자전거 이용에 영향을 미치는 물리적 환경 요인 분석(사경은, 이수기)
    - 네트워크 분석을 통한 공공자전거 따릉이의 이용특성에 대한 연구(강경희, 정진우)
    - 이용수요 기반의 서울시 공공자전거 재배치전략 도출(이은탁, 손봉수)
    - 날씨가 자전거 이용에 미치는 영향 분석 (김동준, 신희철, 반죽신, 임형준)