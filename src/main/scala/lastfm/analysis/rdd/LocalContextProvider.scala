package lastfm.analysis.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object LocalContextProvider {

  def apply(jobName: String): SparkContext = {
    SparkContext.getOrCreate(getConfig(jobName))
  }

  def getConfig(jobbName: String): SparkConf = {
    new SparkConf().setAppName(jobbName).setMaster("local[*]")
  }
}
