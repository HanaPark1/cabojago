
# 파일 목록을 얻어옴
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import desc, sum as _sum


# SparkSession 생성
spark = SparkSession.builder.appName('wordCountEda').getOrCreate()
files = spark.sparkContext.wholeTextFiles("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/filtering_after_wordcount/hannam").keys().collect()

result = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([StructField("word", StringType()), StructField("count", IntegerType())]))

# 각 파일마다
for file in files:
    # 파일의 내용을 읽어 DataFrame 생성
    df = spark.read.format("csv").option("header", "true").load(file)

    # 'count' 컬럼을 integer로 변환
    df = df.withColumn("count", df["count"].cast("integer"))

    top40 = df.orderBy(desc('count')).limit(40)
    # 결과 DataFrame에 추가
    result = result.union(top40)

# 같은 단어는 하나의 word로 sum,전체 결과를 'count' 기준으로 정렬
result = result.groupBy("word").agg(_sum("count").alias("count"))
result = result.orderBy(desc('count'))

# 상위 200개만 선택
result = result.limit(200)

result.coalesce(1).write.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/hn_keyword_result", header=True)

# 결과를 화면에 출력
result.show(300)