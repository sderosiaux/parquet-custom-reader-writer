package custom

import java.util

import CustomParquetReader.CustomString
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api._
import org.apache.parquet.schema.{MessageType, Type}

import scala.collection.JavaConverters._

class CustomFullReadSupport extends ReadSupport[CustomString] {

  override def init(context: InitContext): ReadContext = {
    // we request the same schema as the parquet file here
    // but we could request a custom one :
    //    val request = new MessageType("schema",
    //      new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "ts"),
    //      new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "qs"))

    //new AvroSchemaConverter(context.getConfiguration()).convert(context.getFileSchema)

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

      val fields = fileSchema.getFields.asScala
      private val converters = fields.map { new StringConverter(_) }

      trait PrimitiveConverterWithValue[T] extends PrimitiveConverter {
        var result: T = _
      }

      class StringConverter(field: Type) extends PrimitiveConverterWithValue[String] {
        override def addBinary(value: Binary): Unit = { result = value.toStringUsingUTF8 }
        override def addFloat(value: Float): Unit =  { result = value.toString }
        override def addDouble(value: Double): Unit =  { result = value.toString }
        override def addInt(value: Int): Unit =  { result = value.toString }
        override def addBoolean(value: Boolean): Unit =  { result = value.toString.toUpperCase() }
        override def addLong(value: Long): Unit =  { result = value.toString }
      }

      val rootConverter = new GroupConverter {
        var current: CustomString = _

        override def getConverter(fieldIndex: Int): Converter = {
          converters(fieldIndex)
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
          current.copy(value = converters.flatMap(c => Option(c.result).map(_.toString).orElse(Option("n/a"))).mkString(", "))
        }
      }

      override def getRootConverter: GroupConverter = rootConverter
      override def getCurrentRecord: CustomString = rootConverter.getCurrentRecord()
    }
  }

}

