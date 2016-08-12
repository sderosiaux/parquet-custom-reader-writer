This repository contains a simple implementation of a custom ParquetWriter/ParquetReader.

- A parquet file is written by serializing strings by `CustomParquetWriter`.
- `CustomParquetReader` deserializes the file into a case class.

# CustomParquetWriter

`CustomParquetWriter` extends `ParquetWriter` and passes it a custom `WriteSupport`.
The `WriteSupport` is the central piece that reads the records sent to the writer one by one, then write the parquet values using a `RecordConsumer` (automatically provided).

Output:

```
INFO: parquet.hadoop.InternalParquetRecordWriter: Flushing mem columnStore to file. allocated memory: 12
INFO: parquet.hadoop.ColumnChunkPageWriteStore: written 53B for [name] BINARY: 1 values, 20B raw, 20B comp, 1 pages, encodings: [RLE, PLAIN]
```

If we inspect the generated file using `parquet-tools`:

```
$ parquet-tools meta toto.parquet
creator:     parquet-mr (build 6aa21f8776625b5fa6b18059cfebe7549f2e00cb)
extra:       a = toto

file schema: custom
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

name:        REPEATED BINARY R:1 D:1

row group 1: RC:1 TS:53
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

name:         BINARY UNCOMPRESSED DO:0 FPO:4 SZ:53/53/1.00 VC:1 ENC:RLE,PLAIN

$ parquet-tools cat toto.parquet
name = dG90bw==
```

# CustomParquetReader

To read from parquet, we build a `ParquetReader` and passes it a custom `ReadSupport`.
The `ReadSupport` is more complex than the `WriteSupport`.
It must provide a `RecordMaterializer` to the Parquet framework that itself provide all the field converters.
In the example, to keep it simple, we just create a simple `StringConverter` no matter the column read. In practice, every field types has its own `Converter` implementation.

Output: 

```
hadoop config: Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml
file metadata: {a=toto}
file schema: message custom {
  repeated binary name;
}
requested schema: message custom {
  repeated binary name;
}
read support metadata : null

CustomString(toto)

INFO: parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
INFO: parquet.hadoop.ParquetFileReader: reading another 1 footers
INFO: parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
INFO: parquet.hadoop.InternalParquetRecordReader: RecordReader initialized will read a total of 1 records.
INFO: parquet.hadoop.InternalParquetRecordReader: at row 0. reading next block
INFO: parquet.hadoop.InternalParquetRecordReader: block read in memory in 19 ms. row count = 1
```

# Notes

Parquet files can contain any metadata.
They are written in the footer by the ParquetWriter, and read at first by the ParquetReader.

We can ask to read a simpler (less columns) Parquet schema (called `MessageType`) compared to the one of the files, Parquet handles it.

For instance, the parquet-avro extension converts every GenericRecord fields (recursively) into a Parquet field with the corresponding field type.
Then it writes the avro schema in the metadata.
To read it back, it's been provided the schema of writing (by Parquet, reading the metadata), then it can deserialize the other way around, converting Parquet fields to the corresponding Avro fields at the good location.

Basically, `WriteSupport` and `ReadSupport` implementations are simply in/out converters.
