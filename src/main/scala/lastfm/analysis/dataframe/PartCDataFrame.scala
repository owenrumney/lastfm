package lastfm.analysis.dataframe

import org.apache.log4j.Level
import org.apache.log4j.Logger

object PartCDataFrame {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)

    /*
    Not completed
     */
  }
}
