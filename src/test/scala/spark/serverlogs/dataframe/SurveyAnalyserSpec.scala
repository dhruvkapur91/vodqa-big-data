package spark.serverlogs.dataframe

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen, ShouldMatchers}
import spark.ServerLog
import spark.utils.{DataFrameEquality, SharedSQLContext}

class SurveyAnalyserSpec extends FunSuite with ShouldMatchers with GivenWhenThen with SharedSparkContext with DataFrameEquality with BeforeAndAfterAll {


  test("Sample test by creating collections") {
    val sqlContext = new SQLContext(sc)
    Given("A sequence of server logs")
    val serverLog1 = ServerLog(1,1,"A","P","P",-1,-1)
    val serverLog2 = ServerLog(1,2,"A","Q","Q",-1,-1)
    val serverLogs = Seq(serverLog1,serverLog2)
    val inputServerLogs = sqlContext.createDataFrame(serverLogs)

    When("We search for valid search records")
    val outputSearchLogs = ServerLogAnalyzer.validRecordsForSearch(inputServerLogs)

    Then("We get the valid search logs")
    outputSearchLogs should be(inputServerLogs)
  }

  test("Sample test by reading external file") {
    Given("A server logs csv file")
    val sqlContext = new SQLContext(sc)
    val inputServerLogs = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/server_logs")

    When("We search for valid search records")
    val outputSearchLogs = ServerLogAnalyzer.validRecordsForSearch(inputServerLogs)

    Then("We expect the WordCount map to be empty")
    outputSearchLogs should be(inputServerLogs)
  }

}
