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
  val hdfsBase = s"hdfs://nameservice1/user/ubuntu/bigquery/${sdf.format(new Date().getTime)}/$appId"

  Try (SQLParser.listColumns(sql)) match {
    case Success(res) =>
      res match {
        case Some(list) =>
          val tableToColumns = list.filter(!_._1.isDefined).groupBy(x => x._1).map { y =>
            (y._1, y._2.map(z => z._2).toSet)
          }

          val totalCost = tableToColumns.map { x =>
            val tableName = x._1.get
            val columns = x._2
            val path = new Path(s"$hdfsBase/$appId.$tableName")
            val conf = Some(new Configuration())
            val columnSizes = ParquetResolver.getTableColumnSizes(path, conf)
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
