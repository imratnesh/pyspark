# -*- coding: utf-8 -*-

from random import random
from pyspark.sql import SparkSession
from operator import add
 
def create_spark_session(app_name="SparkApplication"):
    spark_session = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    spark_session.sparkContext.setLogLevel("WARN")
    return spark_session
 
def calculate_pi():
    session = create_spark_session()
    sc = session.sparkContext
 
    total_points = 10000004
    numbers = sc.range(0,total_points)
    points = numbers.map(lambda n: (random(), random()))
    circle = points.filter(lambda p: (p[0]*p[0] + p[1]*p[1]) < 1)
    num_inside = circle.count()
    pi = 4*num_inside/total_points
    print("Pi = ", pi)
    return pi
 
def word_count(filename):
    session = create_spark_session()
    sc = session.sparkContext
    lines = session.read.text(filename).rdd.map(lambda r: r[0])
    
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    
    counts.saveAsTextFile("hdfs://localhost:9000/brandNewDir/input2.txt")
    for (word, count) in output:
        print("%s: %i" % (word, count))
        
    session.stop()
    return "Success"

def store_to_hdfs(filename):
    return True

def read_from_hdfs(filename):
    return True

def store_to_local():
    return True

def read_from_local():
    return True
    