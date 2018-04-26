package lastfm.analysis.dataframe.processors

import lastfm.analysis.dataframe.ListeningDataToDf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.concat

class PartADfProcessor {

  def process(spark: SparkSession, dataFilePath: String): DataFrame = {
    val df = ListeningDataToDf.createDataFrame(spark, dataFilePath)
    df.select(col("_c0").alias("userId"), concat(col("_c3"), lit(" - "), col("_c5")))
      .distinct()
      .groupBy("userId")
      .count()
  }
}
