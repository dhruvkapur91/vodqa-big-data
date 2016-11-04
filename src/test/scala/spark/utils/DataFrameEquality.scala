package spark.utils

import org.apache.spark.sql.DataFrame
import spark.utils.DataFrameSuiteBase._

trait DataFrameEquality {

  type DataFrameAssertion = DataFrame => Unit

  def be(expected: DataFrame): DataFrameAssertion = {
    (actual: DataFrame) => assertDataFrameApproximateEquals(expected, actual, 0.1)
  }

  def beOrderIndependent(expected: DataFrame): DataFrameAssertion = {
    (actual: DataFrame) => assertDataFrameApproximateEqualsWithOrderIndependence(expected, actual, 0.1)
  }

  implicit class ColgaTestableDataFrame(actual: DataFrame) {
    def should(dataFrameAssertion: DataFrameAssertion) = dataFrameAssertion(actual)
  }

}
