package lastfm.analysis.dataframe

import org.scalatest.FunSuite

class PartCDataFrameTest extends FunSuite {

  test("missing input causes exception") {
    intercept[AssertionError] {
      PartCDataFrame.main(Array())
    }
    intercept[AssertionError] {
      PartCDataFrame.main(Array("", ""))
    }
    intercept[AssertionError] {
      PartCDataFrame.main(Array("/test/input", ""))
    }
    intercept[AssertionError] {
      PartCDataFrame.main(Array("", "/test/output"))
    }
  }
}
