package lastfm.analysis.processors

import lastfm.analysis.ListeningDataParser
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class PartBProcessor {
  def process(sc: SparkContext, dataFilePath: String): RDD[(String, Int)] = {
    val listenRecords = sc.textFile(dataFilePath)
      .map(ListeningDataParser.getArtistTrackListenData)
      .groupBy(_.userId)

    listenRecords.mapValues(_.toList.distinct.length)
  }
}
