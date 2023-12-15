SET default_character_encoding 'UTF-8';

-- create_table.pig
cafeInfo = LOAD '/user/maria_dev/output_cafelist/cafe_info_list_mangwon.csv' USING PigStorage(',') AS (cafeID:int, cafeName: chararray, visiterCnt: chararray, blogCnt:chararray, addr:chararray);

visiter= LOAD '/user/maria_dev/filtering_after_wordcount/visiter_reviews/visiter_reviews_knlpy_mw_combined.csv' USING PigStorage(',') AS (index:int, cafeName: chararray, cafeID:int, reviews:chararray);

-- Define your_table_name as the desired table name
-- cafeInfo = FOREACH data GENERATE cafeID, cafeName, visiterCnt, blogCnt, addr;

-- Filter records where blogCnt is less than or equal to 10
filtered = FILTER cafeInfo BY (int)blogCnt <= 10;

-- Define your_table_name as the desired table name
filtered_cafe = FOREACH filtered GENERATE cafeID, cafeName, visiterCnt, blogCnt, addr;
DUMP filtered_cafe;

-- Join
joined_table= JOIN filtered_cafe BY cafeID, visiter BY cafeID;

-- Select
result= FOREACH joined_table GENERATE visiter::cafeID, visiter::cafeName, visiter::reviews;

DUMP result;
-- Store the result into the table
STORE result INTO '/user/maria_dev/pig/visiter_mangwon.csv' USING PigStorage(',');
