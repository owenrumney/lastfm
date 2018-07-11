package lastfm.analysis.processors

import lastfm.analysis.ListeningDataParser
import lastfm.analysis.TrackDetail
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
* Returns an RDD of the top 100 songs with the number of times they've been played
 **/
object Top100SongsWithPlayCounts {

  /**
    * @param dataFilePath - the source data path
    * @param sc - the implicitly provided SparkContext
    * @return
    */
  def apply(dataFilePath: String)(implicit sc: SparkContext): RDD[(TrackDetail, Int)] = {
    val listenRecords = sc.textFile(dataFilePath)
      .map(d => (ListeningDataParser.getArtistTrackListenData(d), 1))

    listenRecords.reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .zipWithIndex()
      .filter(_._2 < 100)
      .keys
  }
}
