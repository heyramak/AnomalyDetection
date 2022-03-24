package io.heyram.cassandra.dao

import java.sql.Timestamp
import com.datastax.driver.core.PreparedStatement
import io.heyram.anomaly.Enums
import org.apache.log4j.Logger
import org.apache.spark.sql.Row


object IntrusionDetectionRepository {

  val logger = Logger.getLogger(getClass.getName)

  def cqlTransactionPrepare(db: String, table: String) = {
    s"""
     insert into $db.$table (
       ${Enums.TransactionCassandra.trans_time}
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
       ${Enums.TransactionCassandra.xAttack}

     )
     values(
       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )"""
  }

  def cqlTransactionBind(prepared: PreparedStatement, record: Row) = {
    val bound = prepared.bind()
    bound.setString(Enums.TransactionCassandra.id, record.getAs[String](Enums.TransactionCassandra.id))
    bound.setTimestamp(Enums.TransactionCassandra.trans_time, record.getAs[Timestamp](Enums.TransactionCassandra.trans_time))
    bound.setDouble(Enums.TransactionCassandra.duration, record.getAs[Double](Enums.TransactionCassandra.duration))
    bound.setString(Enums.TransactionCassandra.protocol_type, record.getAs[String](Enums.TransactionCassandra.protocol_type))
    bound.setString(Enums.TransactionCassandra.service, record.getAs[String](Enums.TransactionCassandra.service))
    bound.setString(Enums.TransactionCassandra.flag, record.getAs[String](Enums.TransactionCassandra.flag))
    bound.setDouble(Enums.TransactionCassandra.src_bytes, record.getAs[Double](Enums.TransactionCassandra.src_bytes))
    bound.setDouble(Enums.TransactionCassandra.dst_bytes, record.getAs[Double](Enums.TransactionCassandra.dst_bytes))
    bound.setDouble(Enums.TransactionCassandra.land, record.getAs[Double](Enums.TransactionCassandra.land))
    bound.setDouble(Enums.TransactionCassandra.wrong_fragment, record.getAs[Double](Enums.TransactionCassandra.wrong_fragment))
    bound.setDouble(Enums.TransactionCassandra.urgent, record.getAs[Double](Enums.TransactionCassandra.urgent))
    bound.setDouble(Enums.TransactionCassandra.hot, record.getAs[Double](Enums.TransactionCassandra.hot))
    bound.setDouble(Enums.TransactionCassandra.num_failed_logins, record.getAs[Double](Enums.TransactionCassandra.num_failed_logins))
    bound.setDouble(Enums.TransactionCassandra.logged_in, record.getAs[Double](Enums.TransactionCassandra.logged_in))
    bound.setDouble(Enums.TransactionCassandra.num_compromised, record.getAs[Double](Enums.TransactionCassandra.num_compromised))
    bound.setDouble(Enums.TransactionCassandra.root_shell, record.getAs[Double](Enums.TransactionCassandra.root_shell))
    bound.setDouble(Enums.TransactionCassandra.su_attempted, record.getAs[Double](Enums.TransactionCassandra.su_attempted))
    bound.setDouble(Enums.TransactionCassandra.num_root, record.getAs[Double](Enums.TransactionCassandra.num_root))
    bound.setDouble(Enums.TransactionCassandra.num_file_creations, record.getAs[Double](Enums.TransactionCassandra.num_file_creations))
    bound.setDouble(Enums.TransactionCassandra.num_shells, record.getAs[Double](Enums.TransactionCassandra.num_shells))
    bound.setDouble(Enums.TransactionCassandra.num_access_files, record.getAs[Double](Enums.TransactionCassandra.num_access_files))
    bound.setDouble(Enums.TransactionCassandra.num_outbound_cmds, record.getAs[Double](Enums.TransactionCassandra.num_outbound_cmds))
    bound.setDouble(Enums.TransactionCassandra.is_host_login, record.getAs[Double](Enums.TransactionCassandra.is_host_login))
    bound.setDouble(Enums.TransactionCassandra.is_guest_login, record.getAs[Double](Enums.TransactionCassandra.is_guest_login))
    bound.setDouble(Enums.TransactionCassandra.count, record.getAs[Double](Enums.TransactionCassandra.count))
    bound.setDouble(Enums.TransactionCassandra.srv_count, record.getAs[Double](Enums.TransactionCassandra.srv_count))
    bound.setDouble(Enums.TransactionCassandra.serror_rate, record.getAs[Double](Enums.TransactionCassandra.serror_rate))
    bound.setDouble(Enums.TransactionCassandra.srv_serror_rate, record.getAs[Double](Enums.TransactionCassandra.srv_serror_rate))
    bound.setDouble(Enums.TransactionCassandra.rerror_rate, record.getAs[Double](Enums.TransactionCassandra.rerror_rate))
    bound.setDouble(Enums.TransactionCassandra.srv_rerror_rate, record.getAs[Double](Enums.TransactionCassandra.srv_rerror_rate))
    bound.setDouble(Enums.TransactionCassandra.same_srv_rate, record.getAs[Double](Enums.TransactionCassandra.same_srv_rate))
    bound.setDouble(Enums.TransactionCassandra.diff_srv_rate, record.getAs[Double](Enums.TransactionCassandra.diff_srv_rate))
    bound.setDouble(Enums.TransactionCassandra.srv_diff_host_rate, record.getAs[Double](Enums.TransactionCassandra.srv_diff_host_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_count, record.getAs[Double](Enums.TransactionCassandra.dst_host_count))
    bound.setDouble(Enums.TransactionCassandra.dst_host_srv_count, record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_count))
    bound.setDouble(Enums.TransactionCassandra.dst_host_same_srv_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_same_srv_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_diff_srv_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_diff_srv_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_same_src_port_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_same_src_port_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_srv_diff_host_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_diff_host_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_serror_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_serror_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_srv_serror_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_serror_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_rerror_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_rerror_rate))
    bound.setDouble(Enums.TransactionCassandra.dst_host_srv_rerror_rate, record.getAs[Double](Enums.TransactionCassandra.dst_host_srv_rerror_rate))
    bound.setDouble(Enums.TransactionCassandra.xAttack, record.getAs[Double](Enums.TransactionCassandra.xAttack))

  }

  def cqlTransaction(db: String, table: String, record: Row): String =
    s"""
     insert into $db.$table (
       ${Enums.TransactionCassandra.id}
       ${Enums.TransactionCassandra.trans_time}
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
       ${Enums.TransactionCassandra.xAttack}
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
        ${record.getAs[Double](Enums.TransactionCassandra.urgent)},
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
       '${record.getAs[String](Enums.TransactionCassandra.xAttack)}'
        )"""
}
