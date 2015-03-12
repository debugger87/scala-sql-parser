import java.text.SimpleDateFormat
import java.util.Date

import com.stephentu.sql.SQLParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

/**
 * Created by yangchaozhong on 3/12/15.
 */
object Boot extends App {
  if (args.length < 2) {
    throw new IllegalArgumentException("appId or sql is null")
  }

  val appId = args(0)
  val sql = args(1)
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val date = if (args.length == 3) args(2) else sdf.format(new Date().getTime)
  val hdfsBase = s"hdfs://hb3:8020/user/ubuntu/bigquery/$date/$appId"

  Try (SQLParser.listColumns(sql)) match {
    case Success(res) =>
      res match {
        case Some(list) =>
          val tableToColumns = list.filter(_._1.isDefined).groupBy(x => x._1).map { y =>
            (y._1, y._2.map(z => z._2).toSet)
          }

          println(tableToColumns)

          val totalCost = tableToColumns.map { x =>
            val tableName = x._1.get
            val columns = x._2
            val path = new Path(s"$hdfsBase/$appId.$tableName")
            val conf = new Configuration()
            conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
            conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

            val columnSizes = ParquetResolver.getTableColumnSizes(path, conf)
            println(columnSizes)
            columnSizes.map { m =>
              columns.map(m(_)).sum
            }.getOrElse(0L)
          }.sum

          println(s"$totalCost Bytes")

        case None =>
          println("Nothing need to count! you may cost zero bytes.")
      }
    case Failure(t) =>
      t.printStackTrace
  }
}
