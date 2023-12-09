# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc
import os

# Spark 세션 초기화
spark = SparkSession.builder.appName("blog_word_count").getOrCreate()

# 입력 데이터 파일 경로
input_data_folder = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/konlpy_result/blog_review_result_knlpy/mangwon"
output_data_folder = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/blog_word_count/word_count_mw"

# wholeTextFiles를 사용하여 폴더 내 파일들 읽어오기
file_rdd = spark.sparkContext.wholeTextFiles(input_data_folder)

# 파일 별로 반복
for input_file_path, text_data in file_rdd.collect():
    # 리스트를 텍스트로 변환
    text = text_data.replace("', '", ' ').replace("['", '').replace("']", '')

    # DataFrame으로 변환
    text_df = spark.createDataFrame([(text,)], ["value"])

    # 리스트 형태소를 explode하여 각각의 형태소로 분리
    exploded_data = text_df.select(explode(split(text_df.value, " ")).alias("word"))

    # 각 형태소별로 그룹화하고 개수를 세기
    word_count = exploded_data.groupBy("word").count()

    # 형태소의 등장 빈도에 따라 내림차순으로 정렬
    word_count = word_count.orderBy(desc("count"))

    # 결과 프린트
    word_count.show()
    print("한 카페")

    # CSV 파일로 저장
    file_name = os.path.basename(input_file_path).replace("mw_", "mw_count_").replace("_blog_morph.txt", ".txt")
    word_count.coalesce(1).write.csv(file_name, header=True, mode="overwrite")
    print("저장 완료")

# Spark 세션 종료
spark.stop()