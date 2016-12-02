package justscala

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import org.scalatest.{FunSuite, GivenWhenThen, ShouldMatchers}

class WordCountSpec extends FunSuite with ShouldMatchers with GivenWhenThen {

  test("Should return an empty Map when given empty List") {
    WordCount.wordCount(ListBuffer.empty[String]) should be(empty)
  }

  test("Should give word count when given a non List") {

    Given("A list of sentences")
    val sentence1 = "one sentence"
    val sentence2 = "second sentence"
    val sentences = ListBuffer(sentence1,sentence2)

    When("Word Count is called")
    val wordCount : Map[String,Int] = WordCount.wordCount(sentences)

    Then("Word Count should give right count for each word")
    wordCount.size should be(3)
    wordCount("one") should be(1)
    wordCount("sentence") should be(2)
    wordCount("second") should be(1)
  }
}
