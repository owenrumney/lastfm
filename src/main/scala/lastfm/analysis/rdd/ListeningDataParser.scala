package lastfm.analysis.rdd

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.util.Try

object ListeningDataParser {

  /**
    * Returns a [[ListenEvent]] reflecting the userId, timestamp, artist and track
    *
    * @param row - the [[String]] row from the input file
    * @return [[ListenEvent]]
    */
  def getListenEvents(row: String): ListenEvent = {
    val Array(userId, ts, _, artist, _, track) = row.split("\t")
    val timestamp = Try(LocalDateTime.parse(ts, DateTimeFormatter.ISO_DATE_TIME)).getOrElse(LocalDateTime.MAX)
    ListenEvent(userId, timestamp, artist, track)
  }

  /**
    * Returns tuple of the userId and the track details
    *
    * @param row - the [[String]] row from the the input file
    * @return a tuple of ([[String]], [[TrackDetail]])
    */
  def getUserArtistTrackListenData(row: String): (String, TrackDetail) = {
    val Array(userId, _, _, artist, _, track) = row.split("\t")
    (userId, TrackDetail(artist, track))
  }

  /**
    * Returns an ArtistTractListenRecord
    *
    * @param row - the [[String]] row from the the input file
    * @return a [[TrackDetail]]
    */
  def getArtistTrackListenData(row: String): TrackDetail = {
    val Array(_, _, _, artist, _, track) = row.split("\t")
    TrackDetail(artist, track)
  }

  case class ListenEvent(userId: String, timestamp: LocalDateTime, artist: String, track: String)

  case class TrackDetail(artist: String, track: String)

}
