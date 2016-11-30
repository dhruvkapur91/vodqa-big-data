package spark.serverlogs.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object FilterSearchLogsRDD extends App {

  // spark app in a local mode.
  val conf = new SparkConf().setMaster("local[*]").setAppName("Filter search logs")
  val sc = new SparkContext(conf)

  // creating RDD
  private val serverLogs: RDD[String] = sc.textFile("/Users/dhruvkapur/Projects/vodka/src/main/resources/server_logs_without_headers_old")

  println(s"Number of logs are ???")

  println("Now there are certain logs which aren't search logs, especially those that miss either search location or search filter or both...")
  println("Lets find just the number of search logs...")

  println(s"Number of logs for search are ???")

}
