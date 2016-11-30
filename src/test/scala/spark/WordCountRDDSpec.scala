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

  test("Should return a map of words along with their count") {
    Given("A sequence of sentences")
    val sentenceOne = "one sentence"
    val sentenceTwo = "another sentence"
    val sentences = Seq(sentenceOne,sentenceTwo)
    val sentencesRDD: RDD[String] = sc.parallelize(sentences)

    When("We do word count")
    val wordCount: RDD[(String, Int)] = WordCountRDD.getWordCount(sentencesRDD)

    Then("We expect right count for each word in Map")
    wordCount.count() should be(3)
    wordCount.lookup("one").head should be(1)
    wordCount.lookup("another").head should be(1)
    wordCount.lookup("sentence").head should be(2)
  }
}



