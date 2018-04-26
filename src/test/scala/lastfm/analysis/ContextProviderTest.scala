package lastfm.analysis

import org.scalatest.FunSuite

class ContextProviderTest extends FunSuite {

  test("config create") {
    val context = ContextProvider("testJob")
    assert(context.isLocal)
    assert(!context.isStopped)
  }
}
