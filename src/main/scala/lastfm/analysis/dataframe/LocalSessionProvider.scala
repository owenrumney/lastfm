package lastfm.analysis.dataframe

import org.apache.spark.sql.SparkSession

object LocalSessionProvider {

  def apply(jobName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(jobName)
      .master("local[*]")
      .getOrCreate()
  }
}
