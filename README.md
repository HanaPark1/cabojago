# BigData Programming Term Project (2023-2)

### Project Overview
: 동네별 카페에 대한 여러 리뷰를 크롤링하여 사용자에게 정보를 제공하는 것을 목표로 합니다.

[노션 프로젝트 페이지](https://heathered-felidae-571.notion.site/59b728d8b08c45e6a88a9bb8caa69714?v=3c8d18a732144636a699e861f70889fc&pvs=4)

## Data Collection
[ 데이터 수집 방법 ]
- 크롤링(selenium, bs4)   
   - selenium: 웹 브라우저를 이용, 웹 사이트 제어를 위해 사용
   - bs4: html에서 데이터 추출, html 및 xml 문서를 parsing 하기 위해 사용

[ 개인별 데이터 수집 ]
- 이지은: 동네별 카페 리스트(카페 이름, 카페 타입) 크롤링
- 양채연: 카페의 리뷰 수, 주소, 고유 ID 크롤링
- 김도연: 카페 방문자 리뷰 크롤링
- 박하나: 카페 블로그 리뷰 크롤링

## Project Progress

#### 11/24
- 개인별 데이터 수집 시작

#### 11/29
- 박하나: 크롤링한 동네별 카페 리스트를 사용하여 블로그 리뷰 수집
- 양채연: 크롤링한 동네별 카페 리스트에 카페의 추가 정보(카페 고유 ID, 방문자 리뷰 수, 블로그 리뷰 수, 주소) 추가
  ( `cafelist_동네명.csv` -> `output_cafelist_동네명.csv` )
