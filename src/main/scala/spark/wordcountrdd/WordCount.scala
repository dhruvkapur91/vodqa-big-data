package spark.wordcountrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  // spark app in a local mode.
  val conf = new SparkConf().setMaster("local[*]").setAppName("Word count")
  val sc = new SparkContext(conf)

  val sentenceOne = "This is one sentence"
  val sentenceTwo = "This is another sentence"
  val sentences = Seq(sentenceOne,sentenceTwo)

  // creating RDD
  private val sentencesRDD: RDD[String] = sc.parallelize(sentences)

  sc.stop()

}
