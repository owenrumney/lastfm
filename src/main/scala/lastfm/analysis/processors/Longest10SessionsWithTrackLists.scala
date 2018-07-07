package lastfm.analysis.processors

import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId

import lastfm.analysis.ListeningDataParser
import lastfm.analysis.ListeningDataParser.ListenEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// Extends Serializable to accommodate groupEvents
object Longest10SessionsWithTrackLists {

  val SESSION_THRESHOLD = 20

  def apply(dataFilePath: String)(implicit sc: SparkContext): RDD[Session] = {
    val listenRecords = sc.textFile(dataFilePath)
      .map(ListeningDataParser.getListenEvents)
      .filter(_.timestamp != LocalDateTime.MAX) // strip out any records where the datetime wasn't as expected
      .groupBy(_.userId)
      .mapValues(_.toList.sortBy(_.timestamp.atZone(ZoneId.systemDefault()).toEpochSecond))
      .flatMapValues(groupEvents)
      .sortBy(r => Duration.between(r._2.firstTs, r._2.lastTs).getSeconds, ascending = false)
      .zipWithIndex()
      .filter(_._2 < 10)
      .keys
    listenRecords.values
  }

  val groupEvents: Iterable[ListenEvent] => List[Session] = (events: Iterable[ListenEvent]) => {
    val firstEvent = events.head
    var sessions: List[Session] = List(Session(firstEvent.userId, firstEvent.timestamp, firstEvent.timestamp, List((firstEvent.artist, firstEvent.track))))

    events.tail.foreach(e => {
      if (Duration.between(sessions.head.lastTs, e.timestamp).toMinutes > SESSION_THRESHOLD) {
        sessions = Session(e.userId, e.timestamp, e.timestamp, List((e.artist, e.track))) :: sessions
      } else {
        sessions.head.tracks = (e.artist, e.track) :: sessions.head.tracks
        sessions.head.lastTs = e.timestamp
      }
    })
    sessions
  }

  case class Session(userId: String, firstTs: LocalDateTime, var lastTs: LocalDateTime, var tracks: List[(String, String)])

}