package lastfm.analysis

import lastfm.analysis.processors.PartAProcessor
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object LastFmAnalysisRunner {

  val jobName: String = getClass.getSimpleName
  val LOG: Logger = LoggerFactory.getLogger(jobName)

  def main(args: Array[String]): Unit = {
    var inputFile: String = ""
    var outputPath: String = "file://tmp/lastfm/"
    var part: String = ""

    var list = args.toList
    while (list.nonEmpty) {
      LOG.debug(list.toString)
      list match {
        case "--inputfile" :: value :: tail =>
          inputFile = value.toString
          list = tail
        case "--outputpath" :: value :: tail =>
          outputPath = value.toString
          list = tail
        case "--part" :: value :: tail =>
          part = value.toString
          list = tail
        case _ =>
          printUsageAndExit(1, list)
      }
    }

    if (inputFile.isEmpty) {
      printUsageAndExit(1, "inputfile")
    }
    if (part.isEmpty) {
      printUsageAndExit(1, "part")
    }

    runAnalysis(inputFile, outputPath, part)
  }

  def runAnalysis(inputFile: String, outputPath: String, part: String): Unit = {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName(jobName).setMaster("local[*]"))

    part.toLowerCase match {
      case "a" =>
        val result = new PartAProcessor().process(sc, inputFile)
        result.saveAsTextFile(outputPath)
      case "b" =>
        val result = new PartAProcessor().process(sc, inputFile)
        result.saveAsTextFile(outputPath)
      case "c" =>
        val result = new PartAProcessor().process(sc, inputFile)
        result.saveAsTextFile(outputPath)
      case _ =>
        printUsageAndExit(1, "part")
    }
  }

  def printUsageAndExit(exitCode: Int, parameter: Any = null): Unit = {
    if (parameter != null) {
      System.err.println(s"Uknown/Unsupported/Empty param: $parameter")
    }
    System.err.println(
      """
        |Usage: LastFM Listening Data Analysis [options]
        |Options:
        |  --inputfile INPUTFILE    Input datapath
        |  --outputpath OUTPUTPATH  Output path to send the results
        |  --part PART              A, B or C - specifies the question part to run
      """.stripMargin)
    System.exit(exitCode)
  }
}
