#!/usr/bin/env python
# coding: utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from operator import add
from nltk.stem.porter import PorterStemmer
import string
import sys
import ntpath
from HTMLParser import HTMLParser
import re
from collections import Counter

porter = PorterStemmer()

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return u''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()



# sc.stop() #remove this when submitting on cluster
conf = SparkConf()
conf.setAppName("Part 2 Parquet Snappy - 3")
conf.set("spark.executor.instances", "5")
conf.set("spark.executor.memory", "10g")
conf.set("spark.executor.cores", "4")
sc = SparkContext(conf = conf)
sc.setLogLevel('INFO')



top_words_list = sc.textFile(sys.argv[2]).collect()
top_words_set = set(top_words_list)


invertedIndex = sc.wholeTextFiles(sys.argv[1], 1000)\
                .map(lambda (file, contents): (ntpath.basename(file), strip_tags(contents))) \
                .map(lambda (file, contents): (file, re.sub("[^a-zA-Z ]+", ' ', contents.lower()).split()))\
                .map(lambda (file,contents):(file, [porter.stem(word) for word in contents if porter.stem(word) in top_words_set]))\
                .map(lambda (file, contents): (file, Counter(contents), len(contents), contents))\
                .flatMap(lambda (file, counter, total_w, contents): {(word, (file, float(counter[word])/total_w)) for word in contents})\
                .groupByKey()\
                .map(lambda x: (x[0], list(x[1])))   


spark = SparkSession(sc)
df = invertedIndex.toDF(["word", "posting_list"])
df.write.parquet(sys.argv[3])
