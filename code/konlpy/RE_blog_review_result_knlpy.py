import pandas as pd
from konlpy.tag import Okt
import os
import glob
import re  # 정규 표현식 라이브러리를 import 합니다.

# Okt 객체 생성
okt = Okt()
current_dir = os.getcwd()

file_path = 'data/crawling/cafelist/cafelist_연남동.csv'
data = pd.read_csv(file_path)

stopwords = data['name'].tolist()
split_stopwords = []

for word in stopwords:
    split_stopwords.extend(word.split())
    

print(split_stopwords)
# 불용어 목록 

# 파일 경로 설정
input_directory = os.path.join(current_dir, 'data', 'crawling', 'blog_review', 'blog_review_result_list', '연남동')
output_directory = os.path.join(current_dir, 'data', 'RE_konlpy', 'blog_reviews', 'yeonnam')
filepaths = glob.glob(input_directory + '/*.csv')

for filepath in filepaths:
    # csv 파일 읽기
    try:
        data = pd.read_csv(filepath)
    except pd.errors.EmptyDataError:
        print(f'Skipped empty file: {filepath}')
        continue
    
    # 불용어 제거
    for stopword in split_stopwords:
        for column in data.columns:
            pattern = re.compile(stopword, re.IGNORECASE)  # 대소문자를 구분하지 않는 정규 표현식 패턴을 생성합니다.
            data[column] = data[column].apply(lambda x: pattern.sub('', str(x)))  # 각 불용어를 찾아 제거
    
    # '명사', '형용사'만 뽑기
    for column in data.columns:
        data[column] = data[column].apply(lambda x: [okt.normalize(word[0]) for word in okt.pos(x) if word[1] in ['Noun', 'Adjective']])

    # 결과를 새로운 txt 파일로 저장
    base = os.path.basename(filepath)
    new_filepath = os.path.join(output_directory, f'{os.path.splitext(base)[0]}_stopwords.txt')
    data.to_csv(new_filepath, index=False, sep='\t', header=None)

print("All files have been processed.")
