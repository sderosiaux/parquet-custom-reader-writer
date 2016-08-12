package custom

import java.util

import custom.CustomParquetReader.CustomString
import org.apache.hadoop.conf.Configuration
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.{InitContext, ReadSupport}
import parquet.io.api._
import parquet.schema.MessageType

class CustomReadSupport extends ReadSupport[CustomString] {

  override def init(context: InitContext): ReadContext = {
    // we request the same schema as the parquet file here
    new ReadContext(context.getFileSchema)
  }

  override def prepareForRead(configuration: Configuration, keyValueMetaData: util.Map[String, String], fileSchema: MessageType, readContext: ReadContext): RecordMaterializer[CustomString] = {
    println(s"hadoop config: $configuration")
    println(s"file metadata: $keyValueMetaData")
    println(s"file schema: $fileSchema")
    println()
    println(s"requested schema: ${readContext.getRequestedSchema}")
    println(s"read support metadata : ${readContext.getReadSupportMetadata}")

    new RecordMaterializer[CustomString] {
      class StringConverter extends PrimitiveConverter {
        var result: String = _
        override def addBinary(value: Binary): Unit = { result = value.toStringUsingUTF8 }
      }
      val converter = new StringConverter
      var current: CustomString = _

      override def getRootConverter: GroupConverter = {
        new GroupConverter {
          override def getConverter(fieldIndex: Int): Converter = {
            converter
          }
          override def start(): Unit = {
            current = CustomString("") // init, just for the demo
          }
          override def end(): Unit = {
          }
        }
      }
      override def getCurrentRecord: CustomString = current.copy(converter.result)
    }
  }

}
