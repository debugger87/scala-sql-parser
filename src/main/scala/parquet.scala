import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import parquet.hadoop.metadata.ParquetMetadata
import parquet.hadoop.util.ContextUtil

/**
 * Created by yangchaozhong on 3/12/15.
 */

object ParquetResolver {

  def readMetaData(origPath: Path, configuration: Option[Configuration]): ParquetMetadata = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to read Parquet metadata: path is null")
    }
    val job = new Job()
    val conf = configuration.getOrElse(ContextUtil.getConfiguration(job))
    val fs: FileSystem = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(s"Incorrectly formatted Parquet metadata path $origPath")
    }
    val path = origPath.makeQualified(fs)

    val children = fs.listStatus(path).filterNot { status =>
      val name = status.getPath.getName
      (name(0) == '.' || name(0) == '_') && name != ParquetFileWriter.PARQUET_METADATA_FILE
    }

    children
      // Try any non-"_metadata" file first...
      .find(_.getPath.getName != ParquetFileWriter.PARQUET_METADATA_FILE)
      // ... and fallback to "_metadata" if no such file exists (which implies the Parquet file is
      // empty, thus normally the "_metadata" file is expected to be fairly small).
      .orElse(children.find(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE))
      .map(ParquetFileReader.readFooter(conf, _))
      .getOrElse(
        throw new IllegalArgumentException(s"Could not find Parquet metadata at path $path"))
  }
}
