package spark.serverlogs.dataframe

import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
//import org.apache.spark.{SparkConf, SparkContext}

object ServerLogAnalyzer extends App {

  // spark app in a local mode.
 /* val conf = new SparkConf().setMaster("local[*]").setAppName("Filter search logs")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  // dataframe
  val serverLogs = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/server_logs")

  println("We can see the server logs!")
  serverLogs.show()
  println("Lets find just the number of search logs...")

  println(s"Number of logs for search are ??? ")*/

  def validRecordsForSearch(serverLogs : DataFrame) = {
    val searchLocationPresent = serverLogs("searchLocation") !== ""
    val searchLocationAbsent = serverLogs("searchLocation") === ""
    val locationFilterAbsent = serverLogs("locationFilter") === ""
    val locationFilterPresent = serverLogs("locationFilter") !== ""

    val searchLocationPresentButFilterAbsent = searchLocationPresent and locationFilterAbsent
    val searchLocationAbsentButFilterPresent = searchLocationAbsent and locationFilterPresent

    serverLogs.where(not(searchLocationPresentButFilterAbsent and searchLocationAbsentButFilterPresent))
  }

  def mostSearchedLocation(serverLogs: DataFrame) :  DataFrame = {
    validRecordsForSearch(serverLogs).agg(max("searchLocation") as "mostSearchedLocation", count("searchLocation") as "numberOfHits")
  }


}
