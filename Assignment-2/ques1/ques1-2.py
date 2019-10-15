# Databricks notebook source
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# COMMAND ----------

def friendPairs(x):
    friendPairs = list()
    if len(x) > 1:
      friends = x[1].split(",")
      for friend1 in friends:
          if x[0] == friend1 or str(x[0]) == '' or str(friend1) == '':
              continue
          pair =  (int(x[0]) < int(friend1) ) and [x[0], friend1] or [friend1, x[0]]
          for friend2 in friends:
              if friend2 != friend1:
                finalPair = [pair[0],pair[1],friend2]
                friendPairs.append(finalPair)
    return friendPairs


df = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj_1_-33625.txt").map(lambda line: line.split("\t")).flatMap(friendPairs).toDF()
df = df.select(df._1.alias('User_A'), df._2.alias('User_B'), df._3.alias("Mutual_Friends"))
df = df.groupBy('User_A', 'User_B', "Mutual_Friends").count()
df = df.filter(df["count"] == 2).groupBy("User_A", "User_B").count()
df = df.select(func.col("User_A"),func.col("User_B"),func.col("count").alias("Mutual/Common Friend Number"))
df.rdd.repartition(1).saveAsTextFile("/FileStore/tables/output12.txt")

# COMMAND ----------

df.show()
