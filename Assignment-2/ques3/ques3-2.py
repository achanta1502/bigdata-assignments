# Databricks notebook source
# data is in the same folder of this script
# load data
review = sc.textFile("FileStore/tables/review.csv").map(lambda line: line.split("::")).toDF()
review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'), review._4.alias('stars'))
business= sc.textFile("FileStore/tables/business.csv").map(lambda line: line.split("::")).toDF()
business = business.select(business._1.alias('business_id'), business._2.alias('full_address'), business._3.alias('categories'))
user = sc.textFile("FileStore/tables/user.csv").map(lambda line: line.split("::")).toDF()
user = user.select(user._1.alias('user_id'), user._2.alias('name'))

#This is also spark.sql function where you can do the same things with SQL query syntax
review.createOrReplaceTempView('review')
business.createOrReplaceTempView('business')
user.createOrReplaceTempView('user')
df1 = spark.sql('select business_id from business where categories like "%Colleges & Universities%"')
df1.createOrReplaceTempView('businessID')
df2 = spark.sql('select review.user_id AS User_id, user.name As Name, review.stars AS Rating from review join businessID ON review.business_id=businessID.business_id join user ON user.user_id=review.user_id')
df2.createOrReplaceTempView('userID')
#df2.show()
df2.rdd.repartition(1).saveAsTextFile("FileStore/tables/Output32.txt")
