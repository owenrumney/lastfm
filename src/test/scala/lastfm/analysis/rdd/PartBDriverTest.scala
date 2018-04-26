package lastfm.analysis.rdd

import org.scalatest.FunSuite

class PartBDriverTest extends FunSuite {

  test("missing input causes exception") {
    intercept[AssertionError] {
      PartBDriver.main(Array())
    }
    intercept[AssertionError] {
      PartBDriver.main(Array("", ""))
    }
    intercept[AssertionError] {
      PartBDriver.main(Array("/test/input", ""))
    }
    intercept[AssertionError] {
      PartBDriver.main(Array("", "/test/output"))
    }
  }
}