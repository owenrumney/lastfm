package lastfm.analysis

import java.time.LocalDateTime

import org.scalatest.FunSuite

class SessionExtensionsTest extends FunSuite {

  implicit val MaxGapBetweenTracksInSameSession: Int = 20

  test("SessionCalculator should return true it track in session") {
    val inSession = SessionExtensions.SessionCalculator(ListenEvent("testUser",
      LocalDateTime.of(2017, 1, 1, 10, 10, 0),
      "artist",
      "song"))
      .inSession(Session("testUser",
        LocalDateTime.of(2017, 1, 1, 9, 0, 0),
        LocalDateTime.of(2017, 1, 1, 10, 0, 0), List.empty))

    assert(inSession)
  }

  test("SessionCalculator should return false it track not in session") {
    val inSession = SessionExtensions.SessionCalculator(ListenEvent("testUser",
      LocalDateTime.of(2017, 1, 1, 11, 10, 0),
      "artist",
      "song"))
      .inSession(Session("testUser",
        LocalDateTime.of(2017, 1, 1, 9, 0, 0),
        LocalDateTime.of(2017, 1, 1, 10, 0, 0), List.empty))

    assert(!inSession)
  }

}
