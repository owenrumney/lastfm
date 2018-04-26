package lastfm.analysis.rdd

import lastfm.analysis.rdd.processors.PartAProcessor
import org.apache.log4j.Level
import org.apache.log4j.Logger

object PartADriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)


    val results = new PartAProcessor().process(LocalContextProvider(getClass.getSimpleName), inputFilePath).cache()
    // output raw result to file
    results.saveAsTextFile(outputPath)

    // write to console for output list - Only using because its a small dataset
    results.foreach(r => println(s"${r._1}: ${r._2}"))
  }
}
