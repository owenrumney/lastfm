package lastfm.analysis.processors

import lastfm.analysis.ListeningDataParser
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class PartAProcessor {

  /**
    * Returns the users and the number of unique songs they have played
    *
    * @param sc           - the SparkContext to use
    * @param dataFilePath - the path to the input file
    * @return - An RDD with a map of the userId [[String]] and unique count [[Int]]
    */
  def process(sc: SparkContext, dataFilePath: String): RDD[(String, Int)] = {
    val listenRecords = sc.textFile(dataFilePath)
      .map(ListeningDataParser.getUserArtistTrackListenData)
      .distinct()

    listenRecords
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
  }
}
