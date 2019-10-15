// Databricks notebook source
val business = sc.textFile("FileStore/tables/business.csv").map(line=>line.split("::"))
val review = sc.textFile("FileStore/tables/review.csv").map(line=>line.split("::"))
val user = sc.textFile("FileStore/tables/user.csv").map(line=>line.split("::"))

// COMMAND ----------

val filterBusiness = business.filter(x => x(2).contains("Colleges & Universities")).map(x => (x(0).toString, x(2).toString))
val getReviews = review.map(x => (x(2).toString, (x(1).toString, x(3).toString)))
val partialOut = filterBusiness.join(getReviews).map(x => (x._2._2._1, x._2._2._2))
val getUsers = user.map(x => (x(0).toString, x(1).toString))
val output = partialOut.join(getUsers).map(x => s"${x._1}\t${x._2._2}\t${x._2._1}")

// COMMAND ----------

//output.collect().foreach(println)
output.coalesce(1).saveAsTextFile("/FileStore/tables/output31.txt")
