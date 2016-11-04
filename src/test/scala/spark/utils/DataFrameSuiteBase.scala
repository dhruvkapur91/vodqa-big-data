package spark.utils

import java.math.BigDecimal
import java.lang.{Double, Float}


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}

import scala.annotation.tailrec
import scala.math._

trait ImplicitConversions {
  implicit def stringToSomeString(s: String): Option[String] = Some(s)
  implicit def doubleToSomeDouble(d: Double): Option[Double] = Some(d)
  implicit def intToSomeInt(i : Int) : Option[Int] = Some(i)
}


object DataFrameSuiteBase extends FlatSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll
  with MockFactory
  with GivenWhenThen
  with ImplicitConversions
{

  def rowApproximatelyEquals(expectedRow: Row, actualRow: Row, tolerance: Double): Boolean = {

    def mayBeColumnMatchAt(index: Int): Option[Boolean] = {
      if (!expectedRow.isNullAt(index)) {
        val expectedColumn = expectedRow.get(index)
        val actualColumn = actualRow.get(index)
        expectedColumn match {
          case b1: Array[Byte] if !equals(b1, actualColumn.asInstanceOf[Array[Byte]])            => Some(false)
          case f1: Float if Float.isNaN(f1) != Float.isNaN(actualColumn.asInstanceOf[Float])     => Some(false)
          case f1: Float if abs(f1 - actualColumn.asInstanceOf[Float]) > tolerance               => Some(false)
          case d1: Double if Double.isNaN(d1) != Double.isNaN(actualColumn.asInstanceOf[Double]) => Some(false)
          case d1: Double if abs(d1 - actualColumn.asInstanceOf[Double]) > tolerance             => Some(false)
          case d1: BigDecimal if d1.compareTo(actualColumn.asInstanceOf[BigDecimal]) != 0        => Some(false)
          case _ if expectedColumn != actualColumn                                               => Some(false)
          case _                                                                                 => None
        }
      } else None
    }

    @tailrec
    def loop(index: Int): Boolean = {
      if (expectedRow.length != actualRow.length) false
      else if (index < expectedRow.length) {
        if (expectedRow.isNullAt(index) != actualRow.isNullAt(index)) return false
        val mayBeColumnMatch = mayBeColumnMatchAt(index)
        mayBeColumnMatch match {
          case Some(result) => result
          case None         => loop(index + 1)
        }
      } else true
    }

    loop(index = 0)
  }

  def zipWithIndex[U](rdd: RDD[U]) = rdd.zipWithIndex().map { case (row, index) => (index, row) }

  def assertDataFrameApproximateEqualsWithOrderIndependence(expected: DataFrame, actual : DataFrame, tolerance : Double) = {
    val expectedColumnNames = expected.schema.fields.map(_.name).map(new Column(_))
    val actualColumnNames = actual.schema.fields.map(_.name).map(new Column(_))
    val expectedSortedOnAllColumns: DataFrame = expected.sort(expectedColumnNames : _*)
    val actualSortedOnAllColumns: DataFrame = actual.sort(actualColumnNames : _*)
    assertDataFrameApproximateEquals(expectedSortedOnAllColumns,actualSortedOnAllColumns,tolerance)
  }

  def assertDataFrameApproximateEquals(expected: DataFrame, actual: DataFrame, tolerance: Double) {
    def logDataFrames = {
      println
      println("Expected: ")
      expected.show()
      println("Actual:")
      actual.show()
    }

    if (!expected.schema.equals(actual.schema)) {
      println(s"Expected schema: ${expected.schema}")
      println(s"Actual schema: ${actual.schema}")
      logDataFrames
    }
    expected.schema should be(actual.schema)

    try {
      expected.rdd.cache
      actual.rdd.cache
      if(!expected.rdd.count.equals(actual.rdd.count())){
        logDataFrames
      }
      expected.rdd.count should be(actual.rdd.count)

      val expectedIndexValue = zipWithIndex(expected.rdd)
      val resultIndexValue = zipWithIndex(actual.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter { case (index, (expectedRow, actualRow)) =>
        !DataFrameSuiteBase.rowApproximatelyEquals(expectedRow, actualRow, tolerance)
      }

      if (!unequalRDD.isEmpty()) {
        logDataFrames

      }
      unequalRDD should be(empty)
    } finally {
      expected.rdd.unpersist()
      actual.rdd.unpersist()
    }
  }
}
