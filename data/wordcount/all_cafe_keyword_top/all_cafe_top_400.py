# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# SparkSession 생성
spark = SparkSession.builder.appName('regionWordCountEda').getOrCreate()

# 파일 목록을 얻어옴
files = spark.sparkContext.wholeTextFiles("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/region_keyword_result/*/*.csv").keys().collect()

region_result = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([StructField("word", StringType()), StructField("count", IntegerType())]))

# 각 파일마다
for file in files:
    # 파일의 내용을 읽어 DataFrame 생성
    df = spark.read.format("csv").option("header", "true").load(file)

    # 'count' 컬럼을 integer로 변환
    df = df.withColumn("count", df["count"].cast("integer"))

    # 같은 단어의 카운트를 합침
    df = df.groupBy("word").agg(_sum("count").alias("count"))

    # 상위 400개 선택
    top400 = df.orderBy(desc('count')).limit(400)

    # 결과 DataFrame에 추가
    region_result = region_result.union(top400)

# 전체 결과를 'count' 기준으로 정렬하고 상위 400개 선택
region_result = region_result.groupBy("word").agg(_sum("count").alias("count"))
region_result = region_result.orderBy(desc('count')).limit(400)

# 결과를 HDFS에 CSV 형식으로 저장
region_result.coalesce(1).write.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/region_keyword_result.csv", header=True)

# 결과를 화면에 출력
region_result.show()