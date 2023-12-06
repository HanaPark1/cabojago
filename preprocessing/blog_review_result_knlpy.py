import pandas as pd
from konlpy.tag import Okt
import os

# Okt 객체 생성
okt = Okt()
current_dir = os.getcwd()

# 파일 경로 설정
filepath = os.path.join(current_dir, 'data', 'blog_review', 'blog_review_result_list', '망원동', 'mw_1580287003_blog.csv')

# csv 파일 읽기
data = pd.read_csv(filepath, header=None, index_col=0)

# 형태소 분석 및 품사 태깅
for column in data.columns:
    data[column] = data[column].apply(lambda x: [word for word, tag in okt.pos(okt.normalize(x), stem=True) if tag in ['Noun', 'Adjective']] if isinstance(x, str) else x)

# 결과 확인
print(data.head())

# 결과를 새로운 csv 파일로 저장
data.to_csv('blog_review_result_list_processing.txt')
