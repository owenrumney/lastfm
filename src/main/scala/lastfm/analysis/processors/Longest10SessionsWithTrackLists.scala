package lastfm.analysis.processors

import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId

import lastfm.analysis.ListeningDataParser
import lastfm.analysis.Session
import lastfm.analysis.ListenEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Get the 10 longest sessions with the tracks that were played in the session
  */
object Longest10SessionsWithTrackLists {

  implicit val MaxAllowedSessionGapBetweenTracks: Int = 20

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
    import lastfm.analysis.SessionExtensions._

    var sessions: List[Session] = {
      val firstEvent = events.head
      List(Session(firstEvent.userId, firstEvent.timestamp, firstEvent.timestamp, List((firstEvent.artist, firstEvent.track))))
    }

    events.tail.foreach(event => {
      if (event inSession sessions.head) {
        sessions.head.tracks = (event.artist, event.track) :: sessions.head.tracks
        sessions.head.lastTs = event.timestamp
      } else {
        sessions = Session(event.userId, event.timestamp, event.timestamp, List((event.artist, event.track))) :: sessions
      }
    })
    sessions
  }
  }
