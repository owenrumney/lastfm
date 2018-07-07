package lastfm.analysis.rdd

import lastfm.analysis.rdd.processors.UniqueSongCountsByUser
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object PartADriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)

    implicit val sparkContext: SparkContext = LocalContextProvider(getClass.getSimpleName)


    val results = UniqueSongCountsByUser(inputFilePath).cache()
    // output raw result to file
    results.saveAsTextFile(outputPath)

    // write to console for output list - Only using because its a small dataset
    results.foreach(r => println(s"${r._1}: ${r._2}"))
  }
}
