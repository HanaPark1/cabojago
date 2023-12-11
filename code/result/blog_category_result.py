import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re


# Spark 세션 생성
spark = SparkSession.builder.appName("ReadCSVFiles").getOrCreate()

# 디렉토리 경로
dir_path = '/home/maria_dev/data/category_result/yeonhee'

# 최종 결과를 저장할 리스트
final_results = []

# 디렉토리와 하위 디렉토리에 있는 모든 파일 조회
for root, dirs, files in os.walk(dir_path):
    for file in files:
        # 파일 확장자가 .csv인 경우만 처리
        if file.endswith('.csv'):
            file_path = os.path.join(root, file)
            # CSV 파일을 DataFrame으로 읽어옴
            # 정규표현식을 사용하여 숫자 추출
            match = re.search(r'yh_count_(\d+)', file_path)
            if match:
                extracted_number = match.group(1)
            else:
                print("No match found.")
            df = spark.read.csv(file_path, header=True)

            # 이색카페 조건 추가
            unique_category_rows = df.filter(df['category'] == '이색').take(1)
            if unique_category_rows:
                # '이색' 카테고리에도 'id' 컬럼 추가
                final_results.extend([{'id': extracted_number, **row.asDict()} for row in unique_category_rows])
            else:
                # 상위 3개 행 중 '공부', '동물', '뷰', '친환경' 중 하나를 포함하는지 확인
                top3_rows = df.limit(3)
                selected_row = top3_rows.filter(
                    (F.col("category").isin(['공부', '동물', '뷰', '친환경'])) &
                    (F.col("count") > 0)
                ).first()

                if selected_row:
                    final_results.append({'id': extracted_number, **selected_row.asDict()})
                else:
                    # 만약 상위 3개 행 중에서 조건에 맞는 행이 없다면 처리
                    if df.count() > 0:
                        final_results.append({'id': extracted_number, **df.limit(1).first().asDict()})

# 최종 결과 출력
for result in final_results:
    print(result)
