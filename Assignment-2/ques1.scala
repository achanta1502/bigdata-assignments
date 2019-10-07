// Databricks notebook source
val file = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj_1_-33625.txt")

// COMMAND ----------

val splitFile = file.map(x => x.split("\t")).filter(x => x.length == 2)

// COMMAND ----------

val mappingData = splitFile.map(x => {
  val user = x(0)
  val friends = x(1).split(",").toList
  for(friend <- friends) yield {
    if(friend.toInt > user.toInt) (user + "," + friend -> friends)
    else (friend + "," + user -> friends)
  }
})

// COMMAND ----------

val flatMap = mappingData.flatMap(identity).map(x => (x._1, x._2)).reduceByKey((x,y) => x.intersect(y)).sortByKey(true)

// COMMAND ----------

val frnd_mutual = flatMap.map(x => (x._1, x._2)).filter(x => x._2.length > 0)
val output = frnd_mutual.map(x => s"${x._1}\t${x._2.length}")

// COMMAND ----------

output.coalesce(1).saveAsTextFile("/FileStore/tables/Output1.txt")

// COMMAND ----------

output.collect().foreach(println)
