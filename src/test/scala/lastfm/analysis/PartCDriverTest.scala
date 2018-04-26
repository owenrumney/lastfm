package lastfm.analysis

import org.scalatest.FunSuite

class PartCDriverTest extends FunSuite {

  test("missing input causes exception") {
    intercept[AssertionError] {
      PartADriver.main(Array())
    }
    intercept[AssertionError] {
      PartCDriver.main(Array("", ""))
    }
    intercept[AssertionError] {
      PartCDriver.main(Array("/test/input", ""))
    }
    intercept[AssertionError] {
      PartCDriver.main(Array("", "/test/output"))
    }
  }
}
