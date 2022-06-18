package io.heyram.cassandra.foreachSink


import io.heyram.cassandra.{CassandraConfig, CassandraDriver}
import io.heyram.anomaly.Enums
import org.apache.spark.sql.{ForeachWriter, Row}
import java.sql.Timestamp


class CassandraSinkForeach(dbName:String, tableName:String) extends ForeachWriter[Row] {

  //val logger = Logger.getLogger(getClass.getName)

  val db: String = dbName
  val table: String = tableName

  private def cqlTransaction(record: Row): String = s"""
     insert into $db.$table (
       ${Enums.TransactionCassandra.id},
       ${Enums.TransactionCassandra.trans_time},
       ${Enums.TransactionCassandra.duration},
       ${Enums.TransactionCassandra.protocol_type},
       ${Enums.TransactionCassandra.service},
       ${Enums.TransactionCassandra.flag},
       ${Enums.TransactionCassandra.src_bytes},
       ${Enums.TransactionCassandra.dst_bytes},
       ${Enums.TransactionCassandra.land},
       ${Enums.TransactionCassandra.wrong_fragment},
       ${Enums.TransactionCassandra.urgent},
       ${Enums.TransactionCassandra.hot},
       ${Enums.TransactionCassandra.num_failed_logins},
       ${Enums.TransactionCassandra.logged_in},
       ${Enums.TransactionCassandra.num_compromised},
       ${Enums.TransactionCassandra.root_shell},
       ${Enums.TransactionCassandra.su_attempted},
       ${Enums.TransactionCassandra.num_root},
       ${Enums.TransactionCassandra.num_file_creations},
       ${Enums.TransactionCassandra.num_shells},
       ${Enums.TransactionCassandra.num_access_files},
       ${Enums.TransactionCassandra.num_outbound_cmds},
       ${Enums.TransactionCassandra.is_host_login},
       ${Enums.TransactionCassandra.is_guest_login},
       ${Enums.TransactionCassandra.count},
       ${Enums.TransactionCassandra.srv_count},
       ${Enums.TransactionCassandra.serror_rate},
       ${Enums.TransactionCassandra.srv_serror_rate},
       ${Enums.TransactionCassandra.rerror_rate},
       ${Enums.TransactionCassandra.srv_rerror_rate},
       ${Enums.TransactionCassandra.same_srv_rate},
       ${Enums.TransactionCassandra.diff_srv_rate},
       ${Enums.TransactionCassandra.srv_diff_host_rate},
       ${Enums.TransactionCassandra.dst_host_count},
       ${Enums.TransactionCassandra.dst_host_srv_count},
       ${Enums.TransactionCassandra.dst_host_same_srv_rate},
       ${Enums.TransactionCassandra.dst_host_diff_srv_rate},
       ${Enums.TransactionCassandra.dst_host_same_src_port_rate},
       ${Enums.TransactionCassandra.dst_host_srv_diff_host_rate},
       ${Enums.TransactionCassandra.dst_host_serror_rate},
       ${Enums.TransactionCassandra.dst_host_srv_serror_rate},
       ${Enums.TransactionCassandra.dst_host_rerror_rate},
       ${Enums.TransactionCassandra.dst_host_srv_rerror_rate},
       ${Enums.TransactionCassandra.xattack}
     )
     values(
       '${record.getAs[String](Enums.TransactionCassandra.id)}',
       '${record.getAs[Timestamp](Enums.TransactionCassandra.trans_time)}',
        ${record.getAs[Double](Enums.TransactionCassandra.duration)},
       '${record.getAs[String](Enums.TransactionCassandra.protocol_type)}',
       '${record.getAs[String](Enums.TransactionCassandra.service)}',
       '${record.getAs[String](Enums.TransactionCassandra.flag)}',
        ${record.getAs[Double](Enums.TransactionCassandra.src_bytes)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_bytes)},
        ${record.getAs[Double](Enums.TransactionCassandra.land)},
        ${record.getAs[Double](Enums.TransactionCassandra.wrong_fragment)},
        ${record.getAs[Double](Enums.TransactionCassandra.urgent)}
        ${record.getAs[Double](Enums.TransactionCassandra.hot)},
        ${record.getAs[Double](Enums.TransactionCassandra.num_failed_logins)},
        ${record.getAs[Double](Enums.TransactionCassandra.logged_in)},
        ${record.getAs[Double](Enums.TransactionCassandra.num_compromised)},
        ${record.getAs[Double](Enums.TransactionCassandra.root_shell)},
        ${record.getAs[Double](Enums.TransactionCassandra.su_attempted)},
        ${record.getAs[Double](Enums.TransactionCassandra.num_root)},
        ${record.getAs[Double](Enums.TransactionCassandra.num_file_creations)},
        ${record.getAs[Double](Enums.TransactionCassandra.num_shells)},
        ${record.getAs[Double](Enums.TransactionCassandra.num_access_files)},
        ${record.getAs[Double](Enums.TransactionCassandra.num_outbound_cmds)},
        ${record.getAs[Double](Enums.TransactionCassandra.is_host_login)},
        ${record.getAs[Double](Enums.TransactionCassandra.is_guest_login)},
        ${record.getAs[Double](Enums.TransactionCassandra.count)},
        ${record.getAs[Double](Enums.TransactionCassandra.srv_count)},
        ${record.getAs[Double](Enums.TransactionCassandra.serror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.srv_serror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.rerror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.srv_rerror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.same_srv_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.diff_srv_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.srv_diff_host_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_count)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_count)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_same_srv_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_diff_srv_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_same_src_port_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_diff_host_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_serror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_serror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_rerror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_rerror_rate)},
        ${record.getAs[Double](Enums.TransactionCassandra.xattack)}
        )"""


  private def cqlOffset(record: Row): String = s"""
     insert into $db.$table (
       ${Enums.TransactionCassandra.kafka_partition},
       ${Enums.TransactionCassandra.kafka_offset}
     )
     values(
        ${record.getAs[Int](Enums.TransactionCassandra.kafka_partition)},
        ${record.getAs[Long](Enums.TransactionCassandra.kafka_offset)}
        )"""

  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    //@TODO command to check if cassandra cluster is up
    true
  }

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/1_connecting.md#connection-pooling
  def process(record: Row): Unit = {
    if (table == CassandraConfig.anomalyTable || table == CassandraConfig.normalTable) {
      println(s"Saving record: $record")
      CassandraDriver.connector.withSessionDo(session =>
        session.execute(cqlTransaction(record))
      )
    }
    else if(table == CassandraConfig.kafkaOffsetTable) {
      println(s"Saving offset to kafka: $record")
      CassandraDriver.connector.withSessionDo(session => {
        session.execute(cqlOffset(record))
      })
    }
  }

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md#cassandra-connection-parameters

  def close(errorOrNull: Throwable): Unit = {

    //CassandraDriver.connector.withClusterDo(session => session.close())
    // close the connection
    //connection.keep_alive_ms	--> 5000ms :	Period of time to keep unused connections open
  }
}
