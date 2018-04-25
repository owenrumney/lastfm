package lastfm.analysis

import java.time.LocalDateTime

import org.scalatest.FunSuite

import scala.io.Source

class ListeningDataParserTest extends FunSuite {
  val CLEAN_SAMPLE_DATA = "src/test/resources/listen_clean_sample.tsv"
  val INCOMPLETE_SAMPLE_DATA = "src/test/resources/listen_incomplete_sample.tsv"

  test("full listen data is loaded from clean tsv file") {
    val result = Source.fromFile(CLEAN_SAMPLE_DATA).getLines().map(ListeningDataParser.getFullListenData).toList
    assert(result.length === 9)
    assert(result.head.userId === "user_001000")
    assert(result.head.artistId === Some("9e53f84d-ef44-4c16-9677-5fd4d78cbd7d"))
    assert(result.head.trackId === Some("55496cad-4dea-4f70-9d8b-7749bada2f7e"))
    assert(result.head.timestamp === LocalDateTime.of(2008, 1, 27, 22, 19, 7))
    assert(result.head.artist === "Wilco")
    assert(result.head.track === "What Light")
  }

  test("artist track listen data is loaded for clean tsv file") {
    val result = Source.fromFile(CLEAN_SAMPLE_DATA).getLines().map(ListeningDataParser.getArtistTrackListenData).toList
    assert(result.length === 9)
    assert(result.head.userId === "user_001000")
    assert(result.head.artist === "Wilco")
    assert(result.head.track === "What Light")
  }

  test("full listen data is loaded from dirty tsv file") {
    val result = Source.fromFile(INCOMPLETE_SAMPLE_DATA).getLines().map(ListeningDataParser.getFullListenData).toList
    assert(result.length === 9)
    assert(result.head.userId === "user_001000")
    assert(result.head.artistId === Some("f1b1cf71-bd35-4e99-8624-24a6e15f133a"))
    assert(result.head.trackId === None)
    assert(result.head.timestamp === LocalDateTime.of(2009, 5, 4, 23, 8, 57))
    assert(result.head.artist === "Deep Dish")
    assert(result.head.track === "Fuck Me Im Famous (Pacha Ibiza)-09-28-2007")
  }

}
