from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc
from pyspark.sql.types import IntegerType

import os

# Spark 세션 생성
spark = SparkSession.builder.appName("KeywordCheck").getOrCreate()

# CSV 파일에서 데이터 로드
keyword_category_df = spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/keyword_category_data.csv", header=True)
folder_path = '/home/maria_dev/cabojago/preprocessing/processing_result/blog_filter_word_after_wordcount/yeonhee'

# 폴더 안에 있는 파일 목록을 얻어옴
file_list = os.listdir(folder_path)

# 파일을 하나씩 순회하며 작업 수행
for file_name in file_list:
    file_path = os.path.join(folder_path, file_name)

    cafe_df = spark.read.csv(file_path, header=True)

    # keyword와 word로 inner join하여 keyword가 cafe.csv에 존재하는지 확인
    result_df = keyword_category_df.join(cafe_df, keyword_category_df["keyword"] == cafe_df["word"], "inner")

    result_df = result_df.withColumn("count", col("count").cast(IntegerType()))
    sum_df = result_df.groupBy("category").sum("count")
    sum_df.orderBy(desc("sum(count)")).show(5)

    output_path = os.path.join('/home/maria_dev/output', file_name)
    sum_df.write.mode('overwrite').csv(output_path, header=True)

# Spark 세션 종료
spark.stop()