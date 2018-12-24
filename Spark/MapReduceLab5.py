from __future__ import print_function
import json
import sys
#not installed
from operator import add
# import findspark
# #findspark.init('C:\Spark\spark-2.2.1-bin-hadoop2.6')
# findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.sql.functions import col, avg

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


#import  org.apache.spark.SparkSession
import random
#not installed
import csv
#standart module
import time
from datetime import datetime, timedelta
from random import randint

import numpy as np
import pandas as pd
from geopy.distance import great_circle

from random_word import RandomWords

import  nltk
import random

DT_FORMAT = '%m/%d/%Y %I:%M %p'

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonLab5")\
        .getOrCreate()

#     mapTop100Drivers = spark.read.json("testfile.csv").rdd \
#                 .filter(lambda x: x.driver_rate is not None) \
#                 .map(lambda x: (x.driver, (int(x.driver_rate), 1))) \
#                 .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
#                 .mapValues(lambda v: v[0] / v[1]) \
#                 .top(100, lambda x: x[1])         

#     file = open("testfileTop100Drivers.csv", "w")
#     for item in json.dumps(mapTop100Drivers).split("],"):
#         file.write("%s]\n" % item)
#     file.close()

# # my variant
#     mapMostDrivingHours=spark.read.json('testfile.csv').rdd \
#             .map(lambda x : (datetime.strptime(x.start_datetime,"%m/%d/%Y %I:%M %p").hour, 1)) \
#             .reduceByKey(lambda x,y : x + y) \
#             .top(5, lambda x : x[1])
 

#     file = open("testfileMostDrivingHours.csv", "w")
#     for item in json.dumps(mapMostDrivingHours).split("],"):
#          file.write("%s]\n" % item)
#     file.close()


#     mapLessRateDrivers = spark.read.json("testfile.csv").rdd \
#              .filter(lambda x: x.driver_rate is not None) \
#              .map(lambda x: (x.driver, (int(x.driver_rate), 1))) \
#              .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
#              .mapValues(lambda v: v[0] / v[1]) \
#              .filter(lambda x: x[1] <= 3.5) \
#              .collect()

#     file = open("testfileLessRateDrivers.csv", "w")
#     for item in json.dumps(mapLessRateDrivers).split("],"):
#          file.write("%s]\n" % item)
#     file.close()

#     mapTop50Clients = spark.read.json("testfile.csv").rdd \
#          .filter(lambda x: x.client_rate is not None) \
#          .map(lambda x: (x.client, (int(x.client_rate), 1))) \
#          .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
#          .mapValues(lambda v: v[0] / v[1]) \
#          .top(50, lambda x: x[1])

#     file = open("testfileTop50Clients.csv", "w")
#     for item in json.dumps(mapTop50Clients).split("],"):
#          file.write("%s]\n" % item)
#     file.close()

#     mapTop100DriversCost = spark.read.json("testfile.csv").rdd \
#          .map(lambda x: (x.driver, (int(x.cost)))) \
#          .reduceByKey(add) \
#          .top(100, lambda x: x[1])

#     file = open("testfileTop100DriversCost.csv", "w")
#     for item in json.dumps(mapTop100DriversCost).split("],"):
#         file.write("%s]\n" % item)
#     file.close()

    nightDrivers = spark.read.json("testfile.csv").rdd \
             .map(lambda x: (x.driver, (datetime.strptime(x.start_datetime, DT_FORMAT).hour))) \
             .mapValues(lambda x: (int(x > 23 or x <= 4), 1)) \
             .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
             .mapValues(lambda v: v[0] / v[1]) \
             .filter(lambda x: x[1] > 0) \
             .collect()
            #  .top(50, lambda x: x[1])
    file = open("nightDrivers.csv", "w")
    for item in json.dumps(nightDrivers).split("],"):
         file.write("%s]\n" % item)
    file.close()

    # DriverCounts = spark.read.json("testfile.csv").rdd \
    #     .map(lambda x: x.driver) \
    #     .distinct() \
    #     .count()

    # file = open("testfileDriverCounts.csv", "w")
    # file.write("%s" % DriverCounts)
    # file.close()

    # ClientCounts = spark.read.json("testfile.csv").rdd \
    #     .map(lambda x: x.client) \
    #     .distinct() \
    #     .count()

    # file = open("testfileClientCounts.csv", "w")
    # file.write("%s" % ClientCounts)
    # file.close()

    # mapTop2PositionDriversGlorify = spark.read.json("testfile.csv").rdd \
    #      .filter(lambda x: x.driver_rate is not None) \
    #      .filter(lambda x: int(x.driver_rate) > 2) \
    #      .map(lambda x: (x.driver_comment, (1))) \
    #      .reduceByKey(add) \
    #      .top(2, lambda x: x[1])

    # file = open("mapTop2PositionDriversGlorify.csv", "w")
    # for item in json.dumps(mapTop2PositionDriversGlorify).split("],"):
    #     file.write("%s]\n" % item)
    # file.close()

    # mapTop2PositionDriversShame = spark.read.json("testfile.csv").rdd \
    #      .filter(lambda x: x.driver_rate is not None) \
    #      .filter(lambda x: int(x.driver_rate) < 3) \
    #      .map(lambda x: (x.driver_comment, (1))) \
    #      .reduceByKey(add) \
    #      .top(2, lambda x: x[1])

    # file = open("mapTop2PositionDriversShame.csv", "w")
    # for item in json.dumps(mapTop2PositionDriversShame).split("],"):
    #     file.write("%s]\n" % item)
    # file.close()


    spark.stop()


