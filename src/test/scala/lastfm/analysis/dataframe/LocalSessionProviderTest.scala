package lastfm.analysis.dataframe

import org.scalatest.FunSuite

class LocalSessionProviderTest extends FunSuite {

  test("config create") {
    val session = LocalSessionProvider("testJob")
    assert(session.sparkContext.isLocal)
    assert(!session.sparkContext.isStopped)
    session.stop()
  }
}

