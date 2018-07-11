package lastfm.analysis

import java.time.LocalDateTime

case class ListenEvent(userId: String, timestamp: LocalDateTime, artist: String, track: String)
