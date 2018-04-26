package lastfm.analysis

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ContextProvider {

  def apply(jobName: String): SparkContext = {
    SparkContext.getOrCreate(getConfig(jobName))
  }

  def getConfig(jobbName: String): SparkConf = {
    new SparkConf().setAppName(jobbName).setMaster("local[*]")
  }
}
