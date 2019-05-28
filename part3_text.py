#!/usr/bin/env python
# coding: utf-8


from pyspark import SparkConf, SparkContext
from operator import add
import string
import sys
import ntpath
import ast
from itertools import combinations

# sc.stop() #remove this when submitting on cluster
conf = SparkConf()
conf.setAppName("Part 3 Text - 3")
conf.set("spark.executor.instances", "15")
conf.set("spark.executor.memory", "10g")
conf.set("spark.executor.cores", "4")
sc = SparkContext(conf = conf)
sc.setLogLevel('INFO')

######just verifying inverted index is 1000
print 'number of words in index = ' + str(len(sc.textFile(sys.argv[1]).collect()))


invertedIndex = sc.textFile(sys.argv[1]).repartition(1000)\
                .map(lambda x: ast.literal_eval(x))\
                .map(lambda (term, docs): (term, sorted(docs, key=lambda x: x[0])))\
                .map(lambda (term, docs): [((doc1[0], doc2[0]), doc1[1]*doc2[1]) for doc1, doc2 in combinations(docs, 2)])\
                .flatMap(lambda list: list)\
                .reduceByKey(add, 1000)\
                .sortBy(lambda x: x[1], ascending=False)


invertedIndex.saveAsTextFile(sys.argv[2])

