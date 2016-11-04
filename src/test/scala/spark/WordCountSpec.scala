package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

object WordCounter {
  def apply(sentences : RDD[String]) : RDD[(String,Int)] = ???
}

class WordCountSpec extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Filter search logs")
  val sc = new SparkContext(conf)

  test("Should return empty RDD given empty RDD") {
    val emptyRDD = sc.parallelize(Seq.empty[String])
    val wordCounterRDD = WordCounter(emptyRDD)
    val wordCounter: Array[(String, Int)] = wordCounterRDD.collect()
  }

  override protected def afterAll(): Unit = {
    sc.stop()
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}
