package shark.execution

import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.conf.HiveConf
import shark.SharkEnv
import org.apache.spark.{SparkEnv, SerializableWritable}
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable, Partition => HivePartition}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import java.sql._
import scala.Array
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.spark.broadcast.Broadcast

/**
 * Created by dryoung on 14-3-20.
 */
class JdbcFlagTableReader(@transient _tableDesc: TableDesc, @transient _localHConf: HiveConf)
  extends TableReader{

  private val _minSplitsPerRDD = _localHConf.getInt("mapred.map.tasks", 1)
  private val _flagList = List(
    "plat",
    "date",
    "timestart",
    "timeend"
  )

  private def closeJdbc(conn : Connection, st : Statement, rs : ResultSet) {
    if (rs != null) {
      try {
        rs.close()
      } catch {
        case e : SQLException =>
      }
    }
    if (st != null) {
      try {
        st.close()
      } catch {
        case e : SQLException =>
      }
    }
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e : SQLException =>
      }
    }
  }

  override def makeRDDForTable(
      hiveTable: HiveTable,
      pruningFnOpt: Option[PruningFunctionType] = None
    ): RDD[_] = {

    val properties = _tableDesc.getProperties()

    val driverClass = properties.getProperty("mapred.jdbc.driver.class")
    val url = properties.getProperty("mapred.jdbc.url")
    val sqlKey = properties.getProperty("mapred.jdbc.sql.key")
    var sqlTemplate = properties.getProperty("mapred.jdbc.sql.template")

    var conn : Connection = null
    var st : Statement = null
    var rs : ResultSet = null

    try {
      Class.forName(driverClass)
      conn = DriverManager.getConnection(url)

      val tableName = hiveTable.getDbName + "." + hiveTable.getTableName
      for (i <- _flagList) {
        var arg = _localHConf.get("mapred.jdbc.sql." + tableName + "." + i)
        if (arg == null) {
          arg = _localHConf.get("mapred.jdbc.sql." + i)
        }

        if (arg != null) {
          sqlTemplate = sqlTemplate.replaceAll("\\{" + i + "\\}", arg)
        }
      }
      if (sqlTemplate == null || sqlTemplate.length == 0) {
        throw new IllegalArgumentException("mapred.jdbc.sql.template:" + sqlTemplate + " is empty")
      }

      val sqlBound = "select max(%s) as _max, min(%s) as _min from %s".format(sqlKey, sqlKey, sqlTemplate)
      logWarning("SQL BOUND:" + sqlBound)

      st = conn.createStatement()
      rs = st.executeQuery(sqlBound)
      if (!rs.next()) {
        throw new SQLException("SQL:" + sqlBound + ",NO DATA")
      }

      val lowerBound = rs.getLong("_min")
      val upperBound = rs.getLong("_max")
      closeJdbc(conn, st, rs)
      conn = null
      st = null
      rs = null

      val fieldList = hiveTable.getFields()
      val fieldNameList = fieldList.map(_.getFieldName)
      val fieldTypeList = fieldList.map(_.getFieldObjectInspector.getTypeName)

      val sqlWhereOrAndFlag = if (sqlTemplate.toLowerCase.indexOf("where") < 0) " where " else " and "

      val sqlQuery = "select %s from %s%s%s between ? and ?".format(
        fieldNameList.mkString(","),
        sqlTemplate,
        sqlWhereOrAndFlag,
        sqlKey)
      logWarning("SQL QUERY:" + sqlQuery)
      logWarning("LOWER BOUND:" + lowerBound)
      logWarning("UPPER BOUND:" + upperBound)
      logWarning("NUM SPLITS:" + _minSplitsPerRDD)
      new JdbcRDD(
        SharkEnv.sc,
        () => {
          Class.forName(driverClass)
          DriverManager.getConnection(url)
        },
        sqlQuery,
        lowerBound,
        upperBound,
        _minSplitsPerRDD,
        (rs : ResultSet) => {
          (0 until fieldTypeList.length).map((i : Int) => {
            val j = i + 1
            val ret = fieldTypeList(i) match {
              case "int" => rs.getInt(j)
              case "float" => rs.getFloat(j)
              case "double" => rs.getDouble(j)
              case _ => {
                var str = ""
                try {
                  str = rs.getString(j)
                } catch {
                  case e : SQLException =>
                }
                str
              }
            }
            ret.asInstanceOf[Object]
          }).toArray
        }
      )
    } catch {
      case e: Exception => throw new RuntimeException("JDBC FAIL: " + e.getMessage)
    } finally {
      closeJdbc(conn, st, rs)
    }
  }

  override def makeRDDForPartitionedTable(
    partitions: Seq[HivePartition],
    pruningFnOpt: Option[PruningFunctionType] = None
    ): RDD[_] = {
      null
  }
}