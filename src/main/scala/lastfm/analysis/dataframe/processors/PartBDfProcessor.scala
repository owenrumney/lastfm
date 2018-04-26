package lastfm.analysis.dataframe.processors

import lastfm.analysis.dataframe.ListeningDataToDf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class PartBDfProcessor {
  def process(spark: SparkSession, dataFilePath: String): DataFrame = {
    val df = ListeningDataToDf.createDataFrame(spark, dataFilePath)
    df.select(col("_c3").alias("artist"), col("_c5").alias("track"))
      .groupBy(col("artist"), col("track"))
      .count().alias("count")
      .orderBy(col("count").desc)
      .limit(100)
  }
}
