import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import parquet.hadoop.metadata.ParquetMetadata

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Created by yangchaozhong on 3/12/15.
 */

object ParquetResolver {

  def readMetaData(origPath: Path, conf: Configuration): ParquetMetadata = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to read Parquet metadata: path is null")
    }

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
      // Only try the "_metadata" file
      .find(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE)
      .map(ParquetFileReader.readFooter(conf, _))
      .getOrElse(
        throw new IllegalArgumentException(s"Could not find Parquet metadata at path $path"))
  }

  def getTableColumnSizes(tablePath: Path,
                         conf: Configuration): Option[Map[String, Long]] = {
    Try {
      val metadata = readMetaData(tablePath, conf)
      val blocks = metadata.getBlocks.asScala

      val pairs = blocks.map { block =>
        val columns = block.getColumns.asScala
        columns.map { column =>
          (column.getPath.asScala.mkString("."), column.getTotalSize)
        }
      }.flatten

      pairs.groupBy(_._1).map { x =>
        (x._1, x._2.map(y => y._2).reduce(_ + _))
      }
    } match {
      case Success(res) => Some(res)
      case Failure(t: Throwable) =>
        t.printStackTrace
        None
    }
  }
}
