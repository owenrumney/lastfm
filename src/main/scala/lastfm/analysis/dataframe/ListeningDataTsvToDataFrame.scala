package lastfm.analysis.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object ListeningDataTsvToDataFrame {
  /**
    * Returns a dataframe reflecting the contents of the input TSV file
    *
    * @param spark        - the SparkSession to work with
    * @param dataFilePath - the file to load
    * @return
    */
  def createDataFrame(spark: SparkSession, dataFilePath: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(dataFilePath)
  }
}
