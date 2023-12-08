# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, desc

import pandas as pd
import os

os.environ['PYTHONIOENDOING'] = 'utf-8'
spark = SparkSession.builder.appName("WordCountBlog").getOrCreate()

text_data = spark.read.text("hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/wordcount_test/blog_review_result/mw_1000020305_blog_morphs.txt")

exploded_data = text_data.select("value", explode(split(text_data.value, ","))).alias("word")

word_count = exploded_data.groupBy("word.col").count()

word_count = word_count.orderBy(desc("count"))

blog_word_count = word_count.toPandas()
print(blog_word_count)