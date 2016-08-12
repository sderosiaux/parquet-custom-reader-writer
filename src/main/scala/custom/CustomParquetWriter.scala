package custom

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter

object CustomParquetWriter extends App {

  // just for the sake of the example, we create a writer to save a String record :-)
  // CustomParquetReader will read it as a case class afterwards
  class parquetWriter(path: Path) extends ParquetWriter[String](path, new CustomWriteSupport(Map("a" -> "toto")))
  val writer = new parquetWriter("/tmp/toto.parquet")
  writer.write("toto")
  writer.close()
}
