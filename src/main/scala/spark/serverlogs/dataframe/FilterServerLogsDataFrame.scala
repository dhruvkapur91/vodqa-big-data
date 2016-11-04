package spark.serverlogs.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FilterServerLogsDataFrame extends App {

  // spark app in a local mode.
  val conf = new SparkConf().setMaster("local[*]").setAppName("Filter search logs")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  // dataframe
  val serverLogs = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Users/dhruvkapur/Projects/vodka/src/main/resources/server_logs")

  println("We can see the server logs!")
  serverLogs.show()

  println(s"Number of logs are ???")

  println("Now there are certain logs which aren't search logs, especially those that miss either search location or search filter or both...")
  println("Lets find just the number of search logs...")

  println(s"Number of logs for search are ??? ")

}
