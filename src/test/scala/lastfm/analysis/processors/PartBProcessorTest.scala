package lastfm.analysis.processors

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class PartBProcessorTest extends FunSuite with BeforeAndAfter {

  val CLEAN_SAMPLE_DATA = "src/test/resources/listen_clean_sample.tsv"
  val INCOMPLETE_SAMPLE_DATA = "src/test/resources/listen_incomplete_sample.tsv"

  var sc: SparkContext = _

  before {
    sc = SparkContext.getOrCreate(new SparkConf().setAppName("testContext").setMaster("local"))
  }

  test("spark context created and local") {
    assert(sc.isLocal)
    assert(!sc.isStopped)
  }

  test("sample data correctly processed") {
    val result = new PartBProcessor().process(sc, CLEAN_SAMPLE_DATA).collect()
    assert(result.length === 8)
    assert(result.head._1.artist === "Wilco")
    assert(result.head._1.track === "Impossible Germany")
    assert(result.head._2 === 2)
  }

  after {
    if (!sc.isStopped) {
      sc.stop()
    }
  }
}
