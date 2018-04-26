package lastfm.analysis.processors

import java.time.ZoneId

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class PartCProcessorTest extends FunSuite with BeforeAndAfter {

  val SESSION_SAMPLE_DATA = "src/test/resources/listen_session_sample.tsv"
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
    val result = new PartCProcessor().process(sc, SESSION_SAMPLE_DATA).collect()
      .sortBy(_.firstTs.atZone(ZoneId.systemDefault()).toEpochSecond)
    assert(result.length === 5)
    assert(result(0).tracks.length === 1)
    assert(result(1).tracks.length === 2)
    assert(result(2).tracks.length === 4)
    assert(result(3).tracks.length === 1)
    assert(result(4).tracks.length === 1)
  }

  after {
    if (!sc.isStopped) {
      sc.stop()
    }
  }
}
