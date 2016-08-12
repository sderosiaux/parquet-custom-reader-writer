package custom

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport

object CustomParquetWriter extends App {

  // just for the sake of the example, we create a writer to save a String record :-)
  // CustomParquetReader will read it as a case class afterwards
  class CustomParquetWriterBuilder(path: Path) extends ParquetWriter.Builder[String, CustomParquetWriterBuilder](path) {
    override def getWriteSupport(conf: Configuration): WriteSupport[String] = new CustomWriteSupport(Map("a" -> "toto"))
    override def self(): CustomParquetWriterBuilder = this
  }
  val writer = new CustomParquetWriterBuilder("/tmp/toto.parquet").withWriteMode(Mode.OVERWRITE).build()
  writer.write("toto")
  writer.close()
}
