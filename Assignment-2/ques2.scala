// Databricks notebook source
val files = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj_1_-33625.txt")

// COMMAND ----------

val file = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj_1_-33625.txt")

val splitFile = file.map(x => x.split("\t")).filter(x => x.length == 2)

val mappingData = splitFile.map(x => {
  val user = x(0)
  val friends = x(1).split(",").toList
  for(friend <- friends) yield {
    if(friend.toInt > user.toInt) (user + "," + friend -> friends)
    else (friend + "," + user -> friends)
  }
})

val flatMap = mappingData.flatMap(identity).map(x => (x._1, x._2)).distinct.reduceByKey((x,y) => x.intersect(y)).sortByKey(true)


// COMMAND ----------

import java.text.SimpleDateFormat
import java.util.Date

// COMMAND ----------

val count = flatMap.map(x => (x._2.length, x._1)).sortByKey(false).take(10).map(x => (x._1,(x._2.split(",")(0),x._2.split(",")(1))))
val userdata = sc.textFile("/FileStore/tables/userdata_1_-2c564.txt")
val parseData = userdata.map(x => {
  val data = x.split(",")
  val userId = data(0)
  val name = data(1)
  val city = data(4)
  val date =data(9) 
  val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
  val pres = dateFormat.parse(date)
  val today = new Date()
  val seconds = Math.abs(today.getTime() - pres.getTime())
  val days = seconds / 86400000
  val age = days / 365
  (userId, (name, city, age))
})

// COMMAND ----------

val countRDD = sc.parallelize(count)
val RDD1 = countRDD.map(x => (x._2._1, (x._2._2,x._1))).join(parseData).map(x => (x._2._1._1, (x._1,x._2._1._2,x._2._2)))
val RDD2 = RDD1.join(parseData).map(x=>(x._2._1._2 ,(x._2._1._3,x._2._2)))
val output = RDD2.map(x=>(x._1+"\t"+x._2._1._1+"\t"+x._2._1._2+"\t"+x._2._1._3+"\t"+x._2._2._1+"\t"+x._2._2._2+"\t"+x._2._2._3) )
//output.collect().foreach(println)
output.coalesce(1).saveAsTextFile("/FileStore/tables/output21.txt")
