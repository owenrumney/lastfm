package lastfm.analysis.rdd

import lastfm.analysis.rdd.processors.PartBProcessor
import org.apache.log4j.Level
import org.apache.log4j.Logger

object PartBDriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)

    val results = new PartBProcessor().process(LocalContextProvider(getClass.getSimpleName), inputFilePath).cache()
    // save the output
    results
      .map(rd => (rd._1.artist, rd._1.track, rd._2))
      .saveAsTextFile(outputPath)

    // write to console for output list - Only using because its a small dataset
    results.foreach(r => println(s"${r._1.artist} - ${r._1.track}: ${r._2}"))
  }
}