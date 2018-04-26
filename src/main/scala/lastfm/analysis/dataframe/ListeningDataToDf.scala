package lastfm.analysis.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object ListeningDataToDf {
  def createDataFrame(spark: SparkSession, dataFilePath: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(dataFilePath)
  }
}
