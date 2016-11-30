package spark

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spark.utils.TestWrapper
import spark.wordcountrdd.WordCountRDD

@RunWith(classOf[JUnitRunner])
class WordCountRDDSpec extends TestWrapper {

  test("Should return empty map when there are no words") {
    Given("An empty sequence of sentences")
    val noSentences = Seq.empty[String]
    val noSentencesRDD: RDD[String] = sc.parallelize(noSentences)

    When("We do word count")
    val wordCount = WordCountRDD.getWordCount(noSentencesRDD)

    Then("We expect WordCount map to be empty")
    wordCount.count() should be(0)
  }

}



