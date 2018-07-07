package lastfm.analysis.dataframe.processors

import lastfm.analysis.dataframe.LocalSessionProvider
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class PartADfProcessorTest extends FunSuite with BeforeAndAfter {

  val CLEAN_SAMPLE_DATA = "src/test/resources/listen_clean_sample.tsv"
  val INCOMPLETE_SAMPLE_DATA = "src/test/resources/listen_incomplete_sample.tsv"

  implicit var spark: SparkSession = _

  before {
    spark = LocalSessionProvider("testContext")
  }

  test("sample data correctly processed") {
    val result = PartADfProcessor(CLEAN_SAMPLE_DATA).collect()
    assert(result.length === 1)
    assert(result.head(0) === "user_001000")
    assert(result.head(1) === 8)
  }
}
