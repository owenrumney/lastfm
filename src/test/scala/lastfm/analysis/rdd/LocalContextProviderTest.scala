package lastfm.analysis.rdd

import org.scalatest.FunSuite

class LocalContextProviderTest extends FunSuite {

  test("config create") {
    val context = LocalContextProvider("testJob")
    assert(context.isLocal)
    assert(!context.isStopped)
    context.stop()
  }
}
