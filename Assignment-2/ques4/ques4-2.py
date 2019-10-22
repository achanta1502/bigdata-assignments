# Databricks notebook source
# data is in the same folder of this script
# load data
review = sc.textFile("FileStore/tables/review.csv").map(lambda line: line.split("::")).toDF()
review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'), review._4.alias('stars'))
business= sc.textFile("FileStore/tables/business.csv").map(lambda line: line.split("::")).toDF()
business = business.select(business._1.alias('business_id'), business._2.alias('full_address'), business._3.alias('categories'))

#This is also spark.sql function where you can do the same things with SQL query syntax
review.createOrReplaceTempView('review')
business.createOrReplaceTempView('business')
df0 = spark.sql('select business_id from business where full_address LIKE "%NY%"')
df0.createOrReplaceTempView('nyid')
df1 = spark.sql('select review.business_id, avg(stars) as avg_rating from review join nyid ON review.business_id = nyid.business_id group by review.business_id ')
df1.createOrReplaceTempView('businessID')
df2 = spark.sql('select distinct business.business_id, full_address, categories, avg_rating from business join businessID ON business.business_id=businessID.business_id order by avg_rating asc limit 20')
#df1.show()
# df2.show()
df2.rdd.repartition(1).saveAsTextFile("FileStore/tables/Output42.txt")
