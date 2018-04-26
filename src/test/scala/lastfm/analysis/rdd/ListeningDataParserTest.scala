package lastfm.analysis.rdd

import java.time.LocalDateTime

import org.scalatest.FunSuite

import scala.io.Source

class ListeningDataParserTest extends FunSuite {
  val CLEAN_SAMPLE_DATA = "src/test/resources/listen_clean_sample.tsv"
  val INCOMPLETE_SAMPLE_DATA = "src/test/resources/listen_incomplete_sample.tsv"

  test("full listen data is loaded from complete tsv file") {
    val result = Source.fromFile(CLEAN_SAMPLE_DATA).getLines().map(ListeningDataParser.getListenEvents).toList
    assert(result.length === 9)
    assert(result.head.userId === "user_001000")
    assert(result.head.timestamp === LocalDateTime.of(2008, 1, 27, 22, 19, 7))
    assert(result.head.artist === "Wilco")
    assert(result.head.track === "What Light")
  }

  test("artist track listen data is loaded by userId for complete tsv file") {
    val result = Source.fromFile(CLEAN_SAMPLE_DATA).getLines().map(ListeningDataParser.getUserArtistTrackListenData).toList
    assert(result.length === 9)
    assert(result.head._1 === "user_001000")
    assert(result.head._2.artist === "Wilco")
    assert(result.head._2.track === "What Light")
  }

  test("artist and track combination loaded from complete tsv file") {
    val result = Source.fromFile(CLEAN_SAMPLE_DATA).getLines().map(ListeningDataParser.getArtistTrackListenData).toList
    assert(result.length === 9)
    assert(result.head.artist === "Wilco")
    assert(result.head.track === "What Light")
  }

  test("full listen data is loaded from dirty tsv file") {
    val result = Source.fromFile(INCOMPLETE_SAMPLE_DATA).getLines().map(ListeningDataParser.getListenEvents).toList
    assert(result.length === 9)
    assert(result.head.userId === "user_001000")
    assert(result.head.timestamp === LocalDateTime.of(2009, 5, 4, 23, 8, 57))
    assert(result.head.artist === "Deep Dish")
    assert(result.head.track === "Fuck Me Im Famous (Pacha Ibiza)-09-28-2007")
  }

}
