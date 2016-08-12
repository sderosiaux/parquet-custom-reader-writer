package custom

import custom.CustomParquetWriter._
import org.apache.hadoop.conf.Configuration
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.io.api.{Binary, RecordConsumer}
import parquet.schema.Type.Repetition
import parquet.schema.{MessageType, PrimitiveType}
import collection.JavaConverters._

class CustomWriteSupport(metadata: Map[String, String]) extends WriteSupport[String] {
  private var consumer: RecordConsumer = _
  val parquetType = new MessageType("custom", new PrimitiveType(Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, "name"))

  override def init(configuration: Configuration): WriteContext = {
    println(configuration)
    new WriteContext(parquetType, metadata.asJava)
  }

  override def write(record: String): Unit = {
    consumer.startMessage()
    consumer.startField("name", 0)
    consumer.addBinary(Binary.fromByteArray(record.getBytes()))
    consumer.endField("name", 0)
    consumer.endMessage()
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    consumer = recordConsumer
  }
}
