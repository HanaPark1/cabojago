import pandas as pd
from konlpy.tag import Okt
import os

# Okt 객체 생성
okt = Okt()
current_dir = os.getcwd()

# 불용어 리스트 생성
stopwords = ['좋다','있다', '같다', '시간', '곳', '것', '로', '수', '맛있다','망원','망원동',
            '층','주문','소설','카페','층','시장','소','아니다','주인공','서울특별시',
            '길','아더','홍대','에러','추천','뭔가','층','길','점','호','경기도','번길','마포구','더',
            '인천광역시','경상남도','스','호점','녀석','밤비','없다','그','대구광역시','영등포구','상가',
            '진짜','이','','거','내','때','저','나','부산광역시','경상도','메','마포','시민','서울','쉬',
            '-','커피','맛','이다','엠','개','이다','안','집','옷','폰','및','또','태형','좋아하다','투썸플레이스',
            '이디야','부동산','한남','한남동','용산구','/','성수동','성수','연희동','연희','연남동','연남',
            '메뉴', '빌딩', '인천', '중구', '귀엽', '친절하다', '향','게','제','밉다','예쁘다','많다','정말'
            ,'하나','가지','곧','사람','사장','방문']

dong_list = ['mangwon']
for dong in dong_list:
    # 파일 경로 설정
    filepath = os.path.join(current_dir, 'data','konlpy', 'visiter_reviews', f'visiter_reviews_knlpy_{dong}.csv')

    # csv 파일 읽기
    data = pd.read_csv(filepath, index_col=0)

    # 형태소 분석 결과를 저장할 DataFrame 생성
    morphs_data = pd.DataFrame(index=data.index, columns=data.columns)

    # 형태소 분석 및 품사 태깅
    for column in data.columns:
        for idx in data.index:
            text = data.loc[idx, column]
            try:
                _ = float(text)  
                morphs_data.loc[idx, column] = text  # 변환 성공하면 그대로 저장
            except ValueError:  # 변환 실패하면 문자열로 처리 data\RE_konlpy\blog_reviews\hannam
                morphs = [word for word, tag in okt.pos(okt.normalize(text), stem=True) if tag in ['Noun', 'Adjective'] and word not in stopwords]
                morphs_data.loc[idx, column] = ' '.join(morphs)

    # 결과 확인
    print(morphs_data.head())
    output_directory = os.path.join(current_dir, 'data', 'RE_konlpy','visiter_reviews',f'{dong}')
    output_filename = os.path.join(output_directory, f'visiter_reviews_knlpy_{dong}.csv')
    
    # 결과를 새로운 csv 파일로 저장
    morphs_data.to_csv(output_filename)
