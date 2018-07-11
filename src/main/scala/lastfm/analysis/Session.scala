package lastfm.analysis

import java.time.LocalDateTime

case class Session(userId: String, firstTs: LocalDateTime, var lastTs: LocalDateTime, var tracks: List[(String, String)])
