package justscala

import org.scalatest.FunSuite

class WordCountSpec extends FunSuite {
  test("Should return count of 1 when given one word") {
    val wordCount: Map[String, Int] = WordCount.apply(Seq("word"))
    assert(wordCount.size == 1)
  }
}
