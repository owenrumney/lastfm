package lastfm.analysis.processors

import lastfm.analysis.LocalContextProvider
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class UniqueSongCountsByUserTest extends FunSuite with BeforeAndAfter {

  val CLEAN_SAMPLE_DATA = "src/test/resources/listen_clean_sample.tsv"
  val INCOMPLETE_SAMPLE_DATA = "src/test/resources/listen_incomplete_sample.tsv"

  implicit var sc: SparkContext = _



  before {
    sc = LocalContextProvider("testContext")
  }

  test("spark context created and local") {
    assert(sc.isLocal)
    assert(!sc.isStopped)
  }

  test("sample data correctly processed") {
    val result = UniqueSongCountsByUser(CLEAN_SAMPLE_DATA).collect()
    assert(result.length === 1)
    assert(result.head._1 === "user_001000")
    assert(result.head._2 === 8)
  }
}
