package spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SharedSQLContext {
  private var _sqlContext : SQLContext = _

  def getSQLContext(sc : SparkContext) = {
    if(_sqlContext == null){
      new SQLContext(sc)
    }else{
      _sqlContext
    }
  }
}
