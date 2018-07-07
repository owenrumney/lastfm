package lastfm.analysis.rdd

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.util.Try

object ListeningDataParser {

  def getListenEvents(row: String): ListenEvent = {
    val Array(userId, ts, _, artist, _, track) = row.split("\t")
    val timestamp = Try(LocalDateTime.parse(ts, DateTimeFormatter.ISO_DATE_TIME)).getOrElse(LocalDateTime.MAX)
    ListenEvent(userId, timestamp, artist, track)
  }

  def getUserArtistTrackListenData(row: String): (String, TrackDetail) = {
    val Array(userId, _, _, artist, _, track) = row.split("\t")
    (userId, TrackDetail(artist, track))
  }

  def getArtistTrackListenData(row: String): TrackDetail = {
    val Array(_, _, _, artist, _, track) = row.split("\t")
    TrackDetail(artist, track)
  }

  case class ListenEvent(userId: String, timestamp: LocalDateTime, artist: String, track: String)

  case class TrackDetail(artist: String, track: String)

}
