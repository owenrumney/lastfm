package lastfm.analysis.rdd

import lastfm.analysis.rdd.processors.UniqueSongCountsByUser
import lastfm.analysis.rdd.processors.Top100SongsWithPlayCounts
import lastfm.analysis.rdd.processors.Longest10SessionsWithTrackLists
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object TaskDriver {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 3)
    val task = args(0)
    val inputFilePath = args(1)
    val outputPath = args(2)
    assert(!inputFilePath.isEmpty)

    implicit val sparkContext: SparkContext = LocalContextProvider(getClass.getSimpleName)

    task.toLowerCase match {
      case "a" => performTaskA(inputFilePath, outputPath)
      case "b" => performTaskB(inputFilePath, outputPath)
      case "c" => performTaskC(inputFilePath, outputPath)
      case _ => throw new UnsupportedOperationException("Task not recognised")
    }
  }

  def performTaskA(inputFilePath: String, outputPath: String)(implicit sc: SparkContext): Unit = {
    val results = UniqueSongCountsByUser(inputFilePath).cache()
    // output raw result to file
    if (!outputPath.isEmpty) {
      results.saveAsTextFile(outputPath)
    }


    // write to console for output list - Only using because its a small dataset
    results.foreach(r => println(s"${r._1}: ${r._2}"))
  }

  def performTaskB(inputFilePath: String, outputPath: String)(implicit sc: SparkContext): Unit = {
    val results = Top100SongsWithPlayCounts(inputFilePath).cache()
    // save the output

    if (!outputPath.isEmpty) {
      results
        .map(rd => (rd._1.artist, rd._1.track, rd._2))
        .saveAsTextFile(outputPath)
    }


    // write to console for output list - Only using because its a small dataset
    results.foreach(r => println(s"${r._1.artist} - ${r._1.track}: ${r._2}"))
  }

  def performTaskC(inputFilePath: String, outputPath: String)(implicit sc: SparkContext): Unit = {
    val results = Longest10SessionsWithTrackLists(inputFilePath).cache()
    // save output
    if (!outputPath.isEmpty) {
      results
        .map(r => (r.userId, r.firstTs, r.lastTs, r.tracks.reverse))
        .saveAsTextFile(outputPath)

    }

    // write to console for output list - Only using because its a small dataset
    results.foreach(r => {
      println(s"UserId: ${r.userId}, Start: ${r.firstTs}, End: ${r.lastTs}")
      r.tracks.reverse.foreach(t => println(s"\t${t._1} - ${t._2}"))
    })
  }
}
