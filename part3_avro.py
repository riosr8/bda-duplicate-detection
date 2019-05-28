#!/usr/bin/env python
# coding: utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from operator import add
import string
import sys
import ntpath
import ast
from itertools import combinations

# sc.stop() #remove this when submitting on cluster
conf = SparkConf()
conf.setAppName("Part 3 Avro - 3")
conf.set("spark.executor.instances", "5")
conf.set("spark.executor.memory", "10g")
conf.set("spark.executor.cores", "4")
sc = SparkContext(conf = conf)
sc.setLogLevel('INFO')
spark = SparkSession(sc)
spark.conf.set("spark.sql.avro.compression.codec", "uncompressed")

df = spark.read.format("com.databricks.spark.avro").load(sys.argv[1])

similarity_matrix = df.rdd.map(lambda x: (x[0], [(y[0], y[1]) for y in x[1]]))\
                .map(lambda (term, docs): (term, sorted(docs, key=lambda x: x[0])))\
                .map(lambda (term, docs): [((doc1[0], doc2[0]), doc1[1]*doc2[1]) for doc1, doc2 in combinations(docs, 2)])\
                .flatMap(lambda list: list)\
                .reduceByKey(add, 1000)\
                .sortBy(lambda x: x[1], ascending=False)


similarity_matrix_df = similarity_matrix.toDF(["word", "posting_list"])
similarity_matrix_df.write.format("com.databricks.spark.avro").save(sys.argv[2])
