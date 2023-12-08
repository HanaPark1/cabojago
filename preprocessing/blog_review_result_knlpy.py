import pandas as pd
from konlpy.tag import Okt
import os
import glob

# Okt 객체 생성
okt = Okt()
current_dir = os.getcwd()

# 파일 경로 설정
directory = os.path.join(current_dir, 'data', 'blog_review', 'blog_review_result_list', '연남동')
filepaths = glob.glob(directory + '/*.csv')

for filepath in filepaths:
    
    # 결과를 새로운 txt 파일로 저장
    output_directory = os.path.join(current_dir, 'preprocessing', 'processing_result','blog_review_result_knlpy','yeonnam')
    output_filename = os.path.join(output_directory, os.path.basename(filepath).replace('.csv', '_morphs.txt'))

    # 파일이 이미 존재하는지 확인
    if os.path.exists(output_filename):
        print(f'Skipped existing file: {output_filename}')
        continue
    
    if os.path.getsize(filepath) < 2:
        print(f'Skipped empty file: {filepath}')
        continue
     # csv 파일 읽기
    try:
        data = pd.read_csv(filepath, header=None, index_col=0)
    except pd.errors.EmptyDataError:
        print(f'Skipped empty file: {filepath}')
        continue
    
    # 형태소 분석 및 품사 태깅
    for column in data.columns:
        data[column] = data[column].apply(lambda x: [word for word, tag in okt.pos(okt.normalize(x), stem=True) if tag in ['Noun', 'Adjective']] if isinstance(x, str) else x)
  
   # 결과 확인
    try:
        print(data.head())
    except UnicodeEncodeError:
        print("UnicodeEncodeError occurred. Skipped printing data.head().")

    # 결과를 새로운 txt 파일로 저장
    data.to_csv(output_filename, sep='\t')
    
