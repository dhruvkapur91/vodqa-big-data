package spark.wordcountrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorldRDD extends App {

  // spark app in a local mode.
  val conf = new SparkConf().setMaster("local[*]").setAppName("Word count")
  val sc = new SparkContext(conf)

  val sentenceOne = "This is one sentence"
  val sentenceTwo = "This is another sentence"
  val sentences = Seq(sentenceOne,sentenceTwo)

  // creating RDD
  val sentencesRDD: RDD[String] = sc.parallelize(sentences)

  val words = sentencesRDD.flatMap(sentence => sentence.split(" "))
  val result = words.map(word => (word, 1)).reduceByKey((word1, word2) => word1 + word2)

  // printing RDD
  result.collect().foreach(println)

}
