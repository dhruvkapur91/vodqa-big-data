package spark.serverlogs.dataframe

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen, ShouldMatchers}
import spark.ServerLog
import spark.utils.{DataFrameEquality, SharedSQLContext}


class SomeTest extends FunSuite with ShouldMatchers {
  test("test something") {
    assert(1 == 1)
    1 should be(1)
  }
}

class SurveyAnalyserSpec extends FunSuite with ShouldMatchers with GivenWhenThen with SharedSparkContext with DataFrameEquality {

  lazy val sqlContext = new SQLContext(sc)

  test("Sample test by creating collections") {
    Given("A sequence of server logs")
    val serverLog = ServerLog(1,1,"A","P","P",-1,-1)
    val serverLogs = Seq(serverLog)
    val inputServerLogs: DataFrame = sqlContext.createDataFrame(serverLogs)

    When("We search for valid search records")
    val outputSearchLogs = ServerLogAnalyzer.validRecordsForSearch(inputServerLogs)

    Then("We get the valid search logs")
    outputSearchLogs should be(inputServerLogs)
  }

  test("Sample test by reading external file") {
    Given("A server logs csv file")
    val inputServerLogs: DataFrame = sqlContext.read
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
