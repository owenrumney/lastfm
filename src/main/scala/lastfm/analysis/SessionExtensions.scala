package lastfm.analysis

import java.time.Duration

object SessionExtensions {
  implicit class SessionCalculator(event: ListenEvent) {
    def inSession(session: Session)(implicit maxGap: Int): Boolean = {
      Duration.between(session.lastTs, event.timestamp).toMinutes < maxGap
    }
  }
}
