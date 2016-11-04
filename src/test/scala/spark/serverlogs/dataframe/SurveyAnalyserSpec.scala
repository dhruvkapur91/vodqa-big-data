package spark.serverlogs.dataframe

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen, ShouldMatchers}
import spark.utils.{DataFrameEquality, SharedSQLContext}

class SurveyAnalyserSpec extends FunSuite with ShouldMatchers with GivenWhenThen with SharedSparkContext with DataFrameEquality with BeforeAndAfterAll {

}
