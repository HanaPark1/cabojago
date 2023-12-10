import os
import pandas as pd

# 현재 작업 디렉터리를 가져옵니다.
current_dir = os.getcwd()

# 시작 디렉터리를 설정합니다.
start_dir = os.path.join(current_dir, 'data', 'blog_review_wordcount', 'mangwon')

# 시작 디렉터리와 그 하위 디렉터리에서 모든 파일을 순회합니다.
for dirpath, dirnames, filenames in os.walk(start_dir):
    for filename in filenames:
        # 파일 확장자를 검사합니다.
        _, extension = os.path.splitext(filename)
        if extension != '.csv':
            continue

        # 파일의 절대 경로를 생성합니다.
        file_path = os.path.join(dirpath, filename)

        try:
            # csv 파일을 데이터프레임으로 불러옵니다.
            df = pd.read_csv(file_path)

            exclude_words = ['좋다','있다', '같다', '시간', '곳', '것', '로', '수', '맛있다','망원','망원동',
                             '층','주문','소설','카페','층','시장','소','아니다','주인공','서울특별시',
                             '길','아더','홍대','에러','추천','뭔가','층','길','점','호','경기도','번길','마포구','더',
                             '인천광역시','경상남도','스','호점','녀석','밤비','없다','그','대구광역시','영등포구','상가',
                             '진짜','이','','거','내','때','저','나','부산광역시','경상도','메','마포','시민','서울','쉬',
                             '-','커피','맛','이다','엠','개','이다','안','집','옷','폰','및','또,','태형','좋아하다','투썸플레이스',
                             '이디야']

            df_filtered = df[~df['word'].isin(exclude_words)]

            # 결과를 새로운 csv 파일로 저장합니다.
            output_directory = os.path.join(current_dir, 'preprocessing', 'processing_result','blog_filter_word_after_wordcount','mangwon')
            output_filename = os.path.join(output_directory, os.path.basename(dirpath).replace('.txt', '_filtered.csv'))
            df_filtered.to_csv(output_filename, index=False)

        except pd.errors.EmptyDataError:
            print(f'{file_path} is empty. Skipping...')
        except pd.errors.ParserError:
            print(f'Error parsing {file_path}. Re-saving the file...')
            df.to_csv(file_path, index=False)
