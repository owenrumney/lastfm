package lastfm.analysis

import lastfm.analysis.processors.PartAProcessor
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PartADriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)


    val results = new PartAProcessor().process(LocalContextProvider(getClass.getSimpleName), inputFilePath)
    // output raw result to file
    results.saveAsTextFile(outputPath)

    // generate a formatted output
    results.foreach(r=>println(s"${r._1}: ${r._2}"))
  }
}
