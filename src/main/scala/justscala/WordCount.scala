package justscala

object WordCount {
  def apply(sentences:Seq[String]) : Map[String,Int] = {
    val words = sentences.flatMap(sentence => sentence.split(" "))
    val groupedWords = words.groupBy(identity)
    groupedWords.mapValues(_.size)
  }
}
