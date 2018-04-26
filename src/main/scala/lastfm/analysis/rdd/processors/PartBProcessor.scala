package lastfm.analysis.rdd.processors

import lastfm.analysis.rdd.ListeningDataParser
import lastfm.analysis.rdd.ListeningDataParser.TrackDetail
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class PartBProcessor {
  /** *
    * Returns the 100 most played songs and the number of times played
    *
    * @param sc           - the SparkContext to use
    * @param dataFilePath - the path to the input file
    * @return - an RDD with the ((artist, track), count))
    */
  def process(sc: SparkContext, dataFilePath: String): RDD[(TrackDetail, Int)] = {
    val listenRecords = sc.textFile(dataFilePath)
      .map(d => (ListeningDataParser.getArtistTrackListenData(d), 1))

    listenRecords.reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .zipWithIndex()
      .filter(_._2 < 100)
      .keys
  }
}
