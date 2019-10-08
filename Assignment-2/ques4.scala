// Databricks notebook source
val business = sc.textFile("FileStore/tables/business.csv").map(line=>line.split("::"))
val review = sc.textFile("FileStore/tables/review.csv").map(line=>line.split("::"))

// COMMAND ----------

val filterBusiness = business.filter(x => x(1).indexOf("NY") >= 0).map(x => (x(0), (x(1), x(2)))).distinct
val getReviews = review.map(x => (x(2), x(3).toDouble)).reduceByKey((x, y) => x + y).distinct
val totalReviews = review.map(x => (x(2), 1)).reduceByKey((x, y) => x + y)
val avgReviews = getReviews.join(totalReviews).map(x => (x._1, x._2._1/x._2._1))
val output = filterBusiness.join(avgReviews).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._2))
val sortOutput = output.collect().sortWith(_._4 < _._4).take(20)

// COMMAND ----------

sc.parallelize(sortOutput).coalesce(1, true).collect().foreach(println)
sc.parallelize(sortOutput).coalesce(1, true).saveAsTextFile("FileStore/tables/output4.txt")
