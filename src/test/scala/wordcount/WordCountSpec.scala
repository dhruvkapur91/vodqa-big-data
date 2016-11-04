package wordcount

import justscala.WordCount
import org.scalatest.{FunSuite, GivenWhenThen, ShouldMatchers}


class WordCountSpec extends FunSuite with ShouldMatchers with GivenWhenThen {
  test("Should return empty map when there are no words") {
    Given("A an empty sequence of sentences")
    val noSentences = Seq.empty[String]

    When("We do word count")
    val wordCount = WordCount(noSentences)

    Then("We expect the WordCount map to be empty")
    wordCount should be(empty)
  }
}



