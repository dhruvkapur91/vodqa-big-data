package spark


import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spark.wordcountrdd.WordCountRDD

@RunWith(classOf[JUnitRunner])
class WordCountSpecWithoutSparkTestingBase extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Filter search logs")
  val sc = new SparkContext(conf)

  test("Should return empty RDD given empty RDD") {
    val emptyRDD = sc.parallelize(Seq.empty[String])
    val wordCounterRDD = WordCountRDD.getWordCount(emptyRDD)
    assert(wordCounterRDD.count() === 0)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}
