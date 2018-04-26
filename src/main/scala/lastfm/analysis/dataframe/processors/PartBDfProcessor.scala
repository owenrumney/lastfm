package lastfm.analysis.dataframe.processors

import lastfm.analysis.dataframe.ListeningDataTsvToDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class PartBDfProcessor {

  /**
    * Returns a dataframe of the top 100 songs by play count
    *
    * @param spark        - the SparkSession to work with
    * @param dataFilePath - the file to load
    * @return
    */
  def process(spark: SparkSession, dataFilePath: String): DataFrame = {
    val df = ListeningDataTsvToDataFrame.createDataFrame(spark, dataFilePath)
    df.select(col("_c3").alias("artist"), col("_c5").alias("track"))
      .groupBy(col("artist"), col("track"))
      .count().alias("count")
      .orderBy(col("count").desc)
      .limit(100)
  }
}
