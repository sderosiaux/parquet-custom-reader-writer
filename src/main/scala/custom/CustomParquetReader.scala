package custom

import ParquetTools._
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader

object CustomParquetReader extends App {

  // just for the example, we encapsulated the string (saved simply as a string by CustomParquetWriter)
  // into a case class. The ReadSupport does the conversion directly.
  case class CustomString(value: String)

  // new ParquetReader(...) is deprecated, we must use the builder form
  def parquetReader(path: Path): ParquetReader[CustomString] = {
    ParquetReader.builder[CustomString](new CustomFullReadSupport, path).build()
    // or we can use CustomFullReadSupport to read every fields
    //ParquetReader.builder[CustomString](new CustomFullReadSupport, path).build()
  }

  val reader = parquetReader("/tmp/294d8dcbc8568cc5-7eaad9794f57b1b2_1968823972_data.0.parq")
  parquetFileIterator(reader).take(10).foreach(println)
  reader.close()

}
