import pandas as pd
from konlpy.tag import Okt
import os

# Okt 객체 생성
okt = Okt()
current_dir = os.getcwd()

# 파일 경로 설정
filepath = os.path.join(current_dir, 'data', 'visiter_reviews', 'visiter_reviews_mangwon.csv')

# csv 파일 읽기
data = pd.read_csv(filepath, index_col=0)

# 형태소 분석 결과를 저장할 DataFrame 생성
morphs_data = pd.DataFrame(index=data.index, columns=data.columns)

# 형태소 분석 및 품사 태깅
for column in data.columns:
    for idx in data.index:
        text = data.loc[idx, column]
        if isinstance(text, str):
            morphs = [word for word, tag in okt.pos(okt.normalize(text), stem=True) if tag in ['Noun', 'Adjective']]
            morphs_data.loc[idx, column] = ' '.join(morphs)

# 결과 확인
print(morphs_data.head())

# 결과를 새로운 csv 파일로 저장
morphs_data.to_csv('visiter_reviews_process_mangwon.csv')
