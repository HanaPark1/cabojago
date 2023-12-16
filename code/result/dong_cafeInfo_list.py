# 분류한 카페와 카페 상세 정보 조인하고, 동 별로 분류에 맞게 나눔

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark 세션 초기화
spark = SparkSession.builder.appName("result").getOrCreate()

dong_list = ['mangwon', 'yeonhee', 'yeonnam', 'hannam','seongsu']

for dong in dong_list:
    cf = ''
    if dong == 'mangwon':
        cf = 'mw'
    elif dong == 'yeonhee':
        cf = 'yh'
    elif dong == 'yeonnam':
        cf = 'yn'
    elif dong == 'hannam':
        cf = 'hn'
    else:
        cf = 'ss'

    cafe_info = spark.read.csv(f"/home/maria_dev/cabojago/data/crawling/cafe_info_list/cafe_info_list_{dong}.csv", header=True, sep=",", inferSchema=True)
    result = spark.read.csv(f"/home/maria_dev/hana/result/{cf}_result.csv", header=True, sep=",", inferSchema=True)

    joinExpression = cafe_info["카페ID"] == result["id"]
    joinType = "inner"
    result = result.join(cafe_info, joinExpression, joinType)

    result = result.select("id", "category", "count", "keyword", "카페이름", "방문자리뷰", "블로그리뷰", "주소")

    #result.show()

    unique_categories = result.select('category').distinct().collect()

    for row in unique_categories:
        category = row['category']

        category_result = result.filter(result.category == category)

        category_result.write.csv(f"/home/maria_dev/hana/result/real_result/{cf}_category_result_{category}", header=True, mode="overwrite")

        # 방문자 리뷰와 블로그 리뷰의 합을 계산하고 정렬
        #category_result = category_result.withColumn("리뷰", col("방문자리뷰") + col("블로그리뷰")).orderBy(col("리뷰").desc())
        #category_result.show(10)