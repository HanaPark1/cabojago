import pandas as pd
from konlpy.tag import Okt
import os

# Okt 객체 생성
okt = Okt()
current_dir = os.getcwd()

# dong_list = ['hannam','mangwon']
dong_list = ['seongsu','yeonnam','yeonhee']

for dong in dong_list:
    # 파일 경로 설정
    filepath = os.path.join(current_dir, 'data', 'visiter_reviews', f'visiter_reviews_{dong}.csv')

    # csv 파일 읽기
    data = pd.read_csv(filepath, index_col=0)

    # 형태소 분석 결과를 저장할 DataFrame 생성
    morphs_data = pd.DataFrame(index=data.index, columns=data.columns)

    # 형태소 분석 및 품사 태깅
    for column in data.columns:
        for idx in data.index:
            text = data.loc[idx, column]
            try:
                _ = float(text)  # 숫자로 변환 시도
                morphs_data.loc[idx, column] = text  # 변환 성공하면 그대로 저장
            except ValueError:  # 변환 실패하면 문자열로 처리
                morphs = [word for word, tag in okt.pos(okt.normalize(text), stem=True) if tag in ['Noun', 'Adjective']]
                morphs_data.loc[idx, column] = ' '.join(morphs)

    # 결과 확인
    print(morphs_data.head())
    output_directory = os.path.join(current_dir, 'preprocessing', 'processing_result','visiter_reviews_knlpy')
    output_filename = os.path.join(output_directory, f'visiter_reviews_knlpy_{dong}.csv')
    
    # 결과를 새로운 csv 파일로 저장
    morphs_data.to_csv(output_filename)
    
