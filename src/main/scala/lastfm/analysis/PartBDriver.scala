package lastfm.analysis

import lastfm.analysis.processors.PartBProcessor
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PartBDriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)

    val results = new PartBProcessor().process(ContextProvider(getClass.getSimpleName), inputFilePath)
    // save the output
    results
      .map(rd => (rd._1.artist, rd._1.track, rd._2))
      .saveAsTextFile(outputPath)

    // generate a formatted output
    results.foreach(r=>println(s"${r._1.artist} - ${r._1.track}: ${r._2}"))
  }
}
