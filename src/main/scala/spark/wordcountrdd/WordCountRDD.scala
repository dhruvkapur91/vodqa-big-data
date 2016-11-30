package spark.wordcountrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountRDD {

  def getWordCount(sentencesRDD : RDD[String]) : RDD[(String, Int)] = {
    val words = sentencesRDD.flatMap(sentence => sentence.split(" "))
    val result: RDD[(String, Int)] = words.map(word => (word, 1)).reduceByKey((word1, word2) => word1 + word2)
    // printing RDD
    result.collect().foreach(println)
    return result
  }
}
