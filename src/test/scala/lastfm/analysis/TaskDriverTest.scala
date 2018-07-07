package lastfm.analysis

import java.nio.file.Files

import org.scalactic.source.Position
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.reflect.io.Path

class TaskDriverTest extends FunSuite with BeforeAndAfter {

  after {
    val testPath = Path(TestOutput)
    if (testPath.exists) testPath.deleteRecursively()
  }

  val cleanSampleData = "src/test/resources/listen_clean_sample.tsv"

  val TestOutput = "/tmp/test_output"

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
  }

  test("Unsupported task id causes an exception") {
    intercept[UnsupportedOperationException] {
      TaskDriver.main(Array("D", "/test/input/", "/test/output"))
    }
  }

  test("Task A runs successful") {
    TaskDriver.main(Array("a", cleanSampleData, ""))

    TaskDriver.main(Array("a", cleanSampleData, TestOutput))
  }

  test("Task B runs successful") {
    TaskDriver.main(Array("b", cleanSampleData, ""))

    TaskDriver.main(Array("b", cleanSampleData, TestOutput))
  }

  test("Task C runs successful") {
    TaskDriver.main(Array("c", cleanSampleData, ""))

    TaskDriver.main(Array("c", cleanSampleData, TestOutput))
  }

}
