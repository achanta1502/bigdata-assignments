// Databricks notebook source
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
val k = 10
val ratingfile = sc.textFile("/FileStore/tables/itemusermat")
val data = ratingfile.map(x=> 
				x.split(" ")).map(x=>(x(0).toInt, Vectors.dense(x.drop(1).map(_.toDouble)))
				)
val dataset: DataFrame = sqlContext.createDataFrame(data).toDF("movie", "features")
val kmeans = new KMeans().setK(k).setFeaturesCol("features").setPredictionCol("clusterid")
val model = kmeans.fit(dataset)
val predictions = model.transform(dataset)
val result = predictions.toDF()
val moviefile = sc.textFile("/FileStore/tables/movies.dat")
val moviedata =  moviefile.map(x=> x.split("::")).map(x=>(x(0).toInt,(x(1),x(2))))
val df2 : DataFrame = sqlContext.createDataFrame(moviedata).toDF("movie", "titlegenre")
val combined = result.join(df2, result("movie")===df2("movie"))
import scala.util.control.Breaks._
for (cluster <- 0 to 9){
  println("Cluster : "+cluster)
  val clusterRows = combined.filter($"clusterid" === cluster).limit(5)
  val output = clusterRows.map(x=> x(0)+" , "+x(4))
  output.collect().foreach(println)
  
}

