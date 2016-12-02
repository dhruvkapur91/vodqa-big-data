package justscala

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map


object WordCount {

  def wordCount(sentences: ListBuffer[String]) : Map[String, Int] = {
    val words = getListOfWords(sentences)
    val wordCount = getCountOfEachWord(words)
    wordCount.foreach(println)
    return wordCount
  }

  def getListOfWords(sentences: ListBuffer[String]) : ListBuffer[String] = {
    var words: mutable.ListBuffer[String] = ListBuffer[String]()
    for (sentence <- sentences) {
      val wordsOfCurrentSentence: Array[String] = sentence.split(" ")
      for (word <- wordsOfCurrentSentence) {
        words += word
      }
    }
    return words
  }

  def getCountOfEachWord(words : ListBuffer[String]) : Map[String, Int] = {
    var wordCount: Map[String, Int] = Map[String, Int]()

    for (word <- words) {
      if (wordCount.isDefinedAt(word)) {
        wordCount(word) = wordCount(word) + 1
      } else {
        wordCount(word) = 1
      }
    }
    return wordCount
  }

}
