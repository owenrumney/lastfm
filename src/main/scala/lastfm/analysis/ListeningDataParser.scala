package lastfm.analysis

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
  * ListenRecord is strongly typed row from source data
  *
  * @param userId
  * @param timestamp
  * @param artistId
  * @param artist
  * @param trackId
  * @param track
  */
case class ListenRecord(userId: String, timestamp: LocalDateTime, artistId: Option[String], artist: String, trackId: Option[String], track: String)

/**
  * ArtistTrackListenRecord is strongly typed subset of the source row
  *
  * @param userId
  * @param artist
  * @param track
  */
case class ArtistTrackListenRecord(userId: String, artist: String, track: String)

object ListeningDataParser {

  /**
    * Returns a [[ListenRecord]]
    *
    * @param row - the [[String]] row from the input file
    * @return [[ListenRecord]]
    */
  def getFullListenData(row: String): ListenRecord = {
    val Array(userId, ts, aId, artist, tId, track) = row.split("\t")
    val timestamp = LocalDateTime.parse(ts, DateTimeFormatter.ISO_DATE_TIME)
    val artistId = if (aId.isEmpty) None else Some(aId)
    val trackId = if (tId.isEmpty) None else Some(tId)
    ListenRecord(userId, timestamp, artistId, artist, trackId, track)
  }

  /**
    * Returns [[ArtistTrackListenRecord]]
    *
    * @param row - the [[String]] row from the the input file
    * @return a [[ArtistTrackListenRecord]]
    */
  def getArtistTrackListenData(row: String): ArtistTrackListenRecord = {
    val Array(userId, ts, aId, artist, tId, track) = row.split("\t")
    ArtistTrackListenRecord(userId, artist, track)
  }


}
