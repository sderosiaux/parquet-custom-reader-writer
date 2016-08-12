package custom

import java.util

import CustomParquetReader.CustomString
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType

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

      val rootConverter = new GroupConverter {
        var current: CustomString = _
        val converter = new StringConverter

        override def getConverter(fieldIndex: Int): Converter = {
          converter
        }
        override def start(): Unit = {
          current = CustomString("") // init, just for the demo
        }
        override def end(): Unit = {
          // used when a leaf of a record was read (when there is a hierarchy in the parquet fields)
          // normally, we must set the record read on the parent, eg. here something like:
          // parentConverter.current.copy(value = s"${current.value} (( ${converter.result} ))")
        }
        def getCurrentRecord() = {
          current.copy(value = converter.result)
        }
      }

      override def getRootConverter: GroupConverter = rootConverter
      override def getCurrentRecord: CustomString = rootConverter.getCurrentRecord()
    }
  }

}
