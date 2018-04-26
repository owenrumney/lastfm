package lastfm.analysis.dataframe

import org.scalatest.FunSuite

class PartBDataFrameTest extends FunSuite {

  test("missing input causes exception") {
    intercept[AssertionError] {
      PartBDataFrame.main(Array())
    }
    intercept[AssertionError] {
      PartBDataFrame.main(Array("", ""))
    }
    intercept[AssertionError] {
      PartBDataFrame.main(Array("/test/input", ""))
    }
    intercept[AssertionError] {
      PartBDataFrame.main(Array("", "/test/output"))
    }
  }
}
