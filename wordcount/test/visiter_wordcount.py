# -*- coding: utf-8 -*-
# 한 카페에 대한 방문자 리뷰 워드카운트 결과를 가짐

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, desc
import os

# UTF-8 인코딩 설정
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("word_count") \
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
    .getOrCreate()

# CSV 파일을 읽어서 DataFrame으로 변환
file_path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/wordcount_test/visiter_reviews_knlpy_mangwon.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 특정 행 선택 (예: 첫 번째 행)
selected_row = df.limit(36).collect()[35]

# 행의 값을 리스트로 가져오기
row_values = list(selected_row.asDict().values())

# None 값을 제거하고, 문자열로 변환 (UTF-8 인코딩 사용)
row_values = [value.encode('utf-8').decode('utf-8') if value is not None else '' for value in row_values]

# 리스트의 값을 문자열로 결합
concatenated_text = " ".join(row_values)

# 문자열을 단어로 분할하고, 워드 카운트 수행
words_df = spark.createDataFrame([(word,) for word in concatenated_text.split(" ")], ["word"])
word_count = words_df.groupBy("word").count().orderBy(desc("count"))

# 결과 출력
word_count.show()

# Spark 세션 종료
spark.stop()
