package lastfm.analysis.dataframe

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class ListeningDataTsvToDataFrameTest extends FunSuite with BeforeAndAfter {
  val CLEAN_SAMPLE_DATA = "src/test/resources/listen_clean_sample.tsv"
  var spark: SparkSession = _

  before {
    spark = LocalSessionProvider("test")
  }

  test("test data loaded as dataframe") {
    val result = ListeningDataTsvToDataFrame.createDataFrame(spark, CLEAN_SAMPLE_DATA).collect()
    assert(result.length === 9)
    assert(result.head(0) === "user_001000")
    assert(result.head(1) === Timestamp.valueOf("2008-01-27 22:19:07.0"))
    assert(result.head(2) === "9e53f84d-ef44-4c16-9677-5fd4d78cbd7d")
    assert(result.head(3) === "Wilco")
    assert(result.head(4) === "55496cad-4dea-4f70-9d8b-7749bada2f7e")
    assert(result.head(5) === "What Light")
  }
}
