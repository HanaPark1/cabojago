# -*- coding: utf-8 -*-

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re
import pandas as pd

# Spark 세션 설정
spark = SparkSession.builder.appName("ReadCSVFiles").getOrCreate()

# 디렉토리 경로
dir_path = '/home/maria_dev/data/category_result/hannam'

# 결과를 저장할 리스트 초기화
final_results = []

# 디렉토리 내 파일들을 순회
for root, dirs, files in os.walk(dir_path):
    for file in files:
        # .csv 파일만 처리
        if file.endswith('.csv'):
            file_path = os.path.join(root, file)
            # 파일명에서 특정 패턴 추출
            match = re.search(r'hn_count_(\d+)', file_path)
            if match:
                extracted_number = match.group(1)
            else:
                print("No match found.")
            
            # Spark DataFrame으로 CSV 파일 읽기
            df = spark.read.csv(file_path, header=True)
            
            # 결과를 리스트에 추가 (한 행만)
            final_results.append({'id': extracted_number, **df.limit(1).first().asDict()})
# 이 결과로 가장 많이 카운트된 카테고리를 뽑아 한 동네의 추이 확인
# Pandas DataFrame으로 변환
final = pd.DataFrame(final_results)

# 카테고리별 카운트 계산
sorted_category_counts = final.groupby('category').agg(category_count=('count', 'count')).reset_index()

# 결과를 CSV로 저장
sorted_category_counts.to_csv('hn_category_count.csv', index=False)

# Spark 세션 종료
spark.stop()
