package lastfm.analysis.rdd

import org.scalatest.FunSuite

class TaskDriverTest extends FunSuite {

  test("missing input causes exception") {
    intercept[AssertionError] {
      TaskDriver.main(Array())
    }
    intercept[AssertionError] {
      TaskDriver.main(Array("", ""))
    }
    intercept[AssertionError] {
      TaskDriver.main(Array("/test/input", ""))
    }
    intercept[AssertionError] {
      TaskDriver.main(Array("", "/test/output"))
    }
  }
}
