import org.apache.hadoop.fs.Path

package object custom {
  implicit def stringToPath(path: String): Path = new Path(path)
}
