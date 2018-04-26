package lastfm.analysis.rdd

import lastfm.analysis.rdd.processors.PartCProcessor
import org.apache.log4j.Level
import org.apache.log4j.Logger

object PartCDriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)

    val results = new PartCProcessor().process(LocalContextProvider(getClass.getSimpleName), inputFilePath).cache()
    // save output
    results
      .map(r => (r.userId, r.firstTs, r.lastTs, r.tracks.reverse))
      .saveAsTextFile(outputPath)

    // write to console for output list - Only using because its a small dataset
    results.foreach(r => {
      println(s"UserId: ${r.userId}, Start: ${r.firstTs}, End: ${r.lastTs}")
      r.tracks.reverse.foreach(t => println(s"\t${t._1} - ${t._2}"))
    })
  }
}