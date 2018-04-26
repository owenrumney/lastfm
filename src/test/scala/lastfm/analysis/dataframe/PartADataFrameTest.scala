package lastfm.analysis.dataframe

import org.scalatest.FunSuite

class PartADataFrameTest extends FunSuite {

  test("missing input causes exception") {
    intercept[AssertionError] {
      PartADataFrame.main(Array())
    }
    intercept[AssertionError] {
      PartADataFrame.main(Array("", ""))
    }
    intercept[AssertionError] {
      PartADataFrame.main(Array("/test/input", ""))
    }
    intercept[AssertionError] {
      PartADataFrame.main(Array("", "/test/output"))
    }
  }
}
