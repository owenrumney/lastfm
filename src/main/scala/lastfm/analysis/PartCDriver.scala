package lastfm.analysis

import lastfm.analysis.processors.PartCProcessor
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PartCDriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)

    val results = new PartCProcessor().process(ContextProvider(getClass.getSimpleName), inputFilePath)
    // save output
    results
      .map(r => (r.firstTs, r.lastTs, r.tracks.reverse))
      .saveAsTextFile(outputPath)

    // print output
    results.foreach(r => {
      println(s"Start: ${r.firstTs}, End: ${r.lastTs}")
      r.tracks.reverse.foreach(t => println(s"\t${t._1} - ${t._2}"))
    })
  }
}
