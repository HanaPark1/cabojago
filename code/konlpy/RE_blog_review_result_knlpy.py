import pandas as pd
from konlpy.tag import Okt
import os
import glob
import re 

okt = Okt()
current_dir = os.getcwd()

file_path = 'data/crawling/cafelist/cafelist_한남동.csv'
data = pd.read_csv(file_path)

stopwords = data['name'].tolist()
split_stopwords = []
split_stopwords.append('한남동')

for word in stopwords:
    split_stopwords.extend(word.split())
    

input_directory = os.path.join(current_dir, 'data', 'wordcount2','mangwon','mw_count_13403633_blog_stopwords.txt')
output_directory = os.path.join(current_dir, 'data', 'RE_konlpy', 'blog_reviews', 'hannam')
filepaths = glob.glob(input_directory + '/*.csv')
for filepath in filepaths:
    try:
        data = pd.read_csv(filepath)
    except pd.errors.EmptyDataError:
        print(f'Skipped empty file: {filepath}')
        continue
    
    for stopword in split_stopwords:
        for column in data.columns:
            if column != 'title':
                pattern = re.compile(stopword, re.IGNORECASE)  
                data[column] = data[column].apply(lambda x: pattern.sub('', str(x)))
    
    for column in data.columns:
        data[column] = data[column].apply(lambda x: [okt.normalize(word[0]) for word in okt.pos(' '.join(okt.morphs(x))) if word[1] in ['Noun', 'Adjective']])
        data[column] = data[column].apply(lambda x: [okt.morphs(word, stem=True)[0] if okt.pos(word)[0][1] == 'Adjective' else word for word in x])
    
    base = os.path.basename(filepath)
    new_filepath = os.path.join(output_directory, f'{os.path.splitext(base)[0]}_stopwords.txt')
    data.to_csv(new_filepath, index=False, sep='\t', header=None)

print("All files have been processed.")
