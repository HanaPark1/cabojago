# 각 동네별 특정 카페 블로그 워드카운드 결과를 키워드로 받아 카테고리별로 매핑
# 그중 상위 5개만 결과로 저장한다

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Spark 세션 생성
spark = SparkSession.builder.appName("KeywordCheck").getOrCreate()

dong_list = ['hannam', 'mangwon', 'seongsu', 'yeonhee', 'yeonnam']

# CSV 파일에서 데이터 로드
keyword_category_df = spark.read.csv("keyword_category.csv", header=True)
for dong in dong_list:
    folder_path = f'/home/maria_dev/cabojago/preprocessing/processing_result/blog_filter_word_after_wordcount/{dong}'

    # 폴더 안에 있는 파일 목록을 얻어옴
    file_list = os.listdir(folder_path)

    # 파일을 하나씩 순회하며 작업 수행
    for file_name in file_list:
        print(file_name)
        file_path = os.path.join(folder_path, file_name)

        cafe_df = spark.read.csv(file_path, header=True)

        # keyword와 word로 inner join하여 keyword가 cafe.csv에 존재하는지 확인
        result_df = keyword_category_df.join(cafe_df, keyword_category_df["keyword"] == cafe_df["word"], "inner")

        # 결과 출력
        result_df = result_df.select("category", "count", "keyword").limit(5)
        file_name = file_name.replace('_blog_morphs_filtered.csv', '')
        # 폴더가 없으면 생성
        if not os.path.exists(f'/home/maria_dev/data/category_result/{dong}/'):
            os.makedirs(f'/home/maria_dev/data/category_result/{dong}/')
        result_df.write.csv(f'/home/maria_dev/data/category_result/{dong}/{file_name}', header=True, mode='overwrite')

# Spark 세션 종료
spark.stop

