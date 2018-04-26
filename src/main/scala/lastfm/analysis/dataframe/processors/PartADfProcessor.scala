package lastfm.analysis.dataframe.processors

import lastfm.analysis.dataframe.ListeningDataTsvToDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.concat

class PartADfProcessor {

  /**
    * Returns a dataframe of the source data with userId and unique songs
    *
    * @param spark        - the SparkSession to work with
    * @param dataFilePath - the file to load
    * @return
    */
  def process(spark: SparkSession, dataFilePath: String): DataFrame = {
    val df = ListeningDataTsvToDataFrame.createDataFrame(spark, dataFilePath)
    df.select(col("_c0").alias("userId"), concat(col("_c3"), lit(" - "), col("_c5")))
      .distinct()
      .groupBy("userId")
      .count()
  }
}
