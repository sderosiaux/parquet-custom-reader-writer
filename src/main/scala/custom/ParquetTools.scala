package custom

import org.apache.parquet.hadoop.ParquetReader

object ParquetTools {

  def parquetFileIterator[T](reader: ParquetReader[T]) = new Iterator[T] {
    private var last: Option[T] = _
    override def hasNext: Boolean = { last = Option(reader.read()); last.isDefined }
    override def next(): T = last.getOrElse(throw new IllegalStateException())
  }

}
