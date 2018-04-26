package lastfm.analysis.dataframe

import lastfm.analysis.dataframe.processors.PartADfProcessor
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PartADataFrame {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    assert(args.length == 2)
    val inputFilePath = args(0)
    val outputPath = args(1)
    assert(!inputFilePath.isEmpty)
    assert(!outputPath.isEmpty)

    val spark = LocalSessionProvider(getClass.getSimpleName)
    val df = new PartADfProcessor().process(spark, inputFilePath).cache()

    // write raw to file
    df.write.csv(outputPath)

    // write to console for output list - Only using because its a small dataset
    df.collect.foreach(r => println(s"${r(0)}: ${r(1)}"))
  }
}
