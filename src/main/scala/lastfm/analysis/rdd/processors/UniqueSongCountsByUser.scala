package lastfm.analysis.rdd.processors

import lastfm.analysis.rdd.ListeningDataParser
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object UniqueSongCountsByUser {

  def apply(dataFilePath: String)(implicit sc: SparkContext) : RDD[(String, Int)] = {
    val listenRecords = sc.textFile(dataFilePath)
      .map(ListeningDataParser.getUserArtistTrackListenData)
      .distinct()

    listenRecords
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
  }
}
