package lastfm.analysis.rdd.processors

import lastfm.analysis.rdd.ListeningDataParser
import lastfm.analysis.rdd.ListeningDataParser.TrackDetail
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Top100SongsWithPlayCounts {

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
