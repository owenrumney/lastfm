package lastfm.analysis.rdd

import org.scalatest.FunSuite

class PartADriverTest extends FunSuite {

  test("missing input causes exception") {
    intercept[AssertionError] {
      PartADriver.main(Array())
    }
    intercept[AssertionError] {
      PartADriver.main(Array("", ""))
    }
    intercept[AssertionError] {
      PartADriver.main(Array("/test/input", ""))
    }
    intercept[AssertionError] {
      PartADriver.main(Array("", "/test/output"))
    }
  }
}
