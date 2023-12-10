# -*- coding: utf-8 -*-

from pyhive import hive

conn = None

input_dir= 'user/maria_dev/wordcount/blog_reviews/'

# Hive 서버에 연결
try:
    conn = hive.Connection(port=10000)
except Exception as e:
    print(f"Error connecting to Hive Server: {e}")

if conn is None:
    exit()

cursor= conn.cursor()

drop_query= '''DROP TABLE IF EXISTS keyword_table'''
cursor.execute(drop_query)
drop_query= '''DROP TABLE IF EXISTS word_count_table'''
cursor.execute(drop_query)



# Hive 테이블 생성 및 데이터 삽입
create_keyword_table_query = '''
CREATE TABLE IF NOT EXISTS keyword_table (
    keyword STRING,
    category STRING
)'''

insert_keyword_data_query = '''
INSERT INTO TABLE keyword_table VALUES
    ('집중', '카공'),
    ('조용', '카공'),
    ('공간', '카공'),
    ('케이크', '디저트'),
    ('크림', '디저트'),
    ('강아지', '애견동반')
'''

create_word_count_table_query = '''
CREATE TABLE IF NOT EXISTS word_count_table (
    word STRING,
    count INT
)'''

insert_word_count_table_query = '''
INSERT INTO TABLE word_count_table VALUES
('케이크', 14),
('커피', 10),
('공간', 9)
'''

# 첫 번째 쿼리 실행
try:
    print("Executing create_keyword_table_query:")
    print(create_keyword_table_query)
    cursor.execute(create_keyword_table_query)
    cursor.execute(insert_keyword_data_query)
except Exception as e:
    print(f"에러 executing create_keyword_table_query: {e}")
    exit()

# 두 번째 쿼리 실행
try:
    print("\nExecuting create_word_count_table_query:")
    print(create_word_count_table_query)
    cursor.execute(create_word_count_table_query)
    cursor.execute(insert_word_count_table_query)

except Exception as e:
    print(f"에러 executing create_word_count_table_query: {e}")
    exit()

# Hive 쿼리 작성 및 실행
query = '''
SELECT wct.word, wct.count, kt.category
FROM word_count_table wct
JOIN keyword_table kt ON wct.word = kt.keyword
'''

cursor.execute(query)

# 결과 출력
for row in cursor.fetchall():
  print(row)

# 연결 종료
conn.close()