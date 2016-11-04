package spark.utils

import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen, Matchers}


trait TestWrapper extends FunSuite
                     with Matchers
                     with BeforeAndAfterAll
                     with GivenWhenThen
                     // with SharedSparkContext

