package custom

import ParquetTools._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

object CustomAvroParquetReader extends App {

  // Use AvroParquetReader to read parquet using a avro schema
  // if no avro schema is provided, parquet-avro creates one from the parquet schema

  case class CustomString(value: String)

  // read all fields
  // by default, parquet-avro will create an avro schema from the parquet schema
  val reader: ParquetReader[_] = AvroParquetReader.builder("/tmp/294d8dcbc8568cc5-7eaad9794f57b1b2_1968823972_data.0.parq").build()
  parquetFileIterator(reader).take(10).foreach(println)
  reader.close()

  // only retrieve the field we want "ip" with a projection schema
  val conf = new Configuration()
  conf.set("parquet.avro.projection", """{"type":"record","name":"schema","fields":[{"name":"ip","type":["null","string"],"default":null}]}""")
  val readerWithProjection = AvroParquetReader.builder("/tmp/294d8dcbc8568cc5-7eaad9794f57b1b2_1968823972_data.0.parq").withConf(conf).build()
  parquetFileIterator(readerWithProjection).take(10).foreach(println)
  reader.close()

}
