package io.heyram.anomaly

import org.apache.spark.sql.types._


object Schema {


  val transactionStructureName = "transaction"

  val transactionSchema: StructType = new StructType()
    .add(Enums.TransactionKafka.id, StringType, nullable = true)
    .add(Enums.TransactionKafka.trans_time, StringType, nullable = true)
    .add(Enums.TransactionKafka.duration, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.protocol_type, StringType, nullable = true)
    .add(Enums.TransactionKafka.service, StringType, nullable = true)
    .add(Enums.TransactionKafka.flag, StringType, nullable = true)
    .add(Enums.TransactionKafka.src_bytes, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_bytes, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.land, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.wrong_fragment, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.urgent, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.hot, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.num_failed_logins, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.logged_in, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.num_compromised, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.root_shell, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.su_attempted, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.num_root, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.num_file_creations, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.num_shells, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.num_access_files, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.num_outbound_cmds, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.is_host_login, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.is_guest_login, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.count, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.srv_count, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.serror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.srv_serror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.rerror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.srv_rerror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.same_srv_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.diff_srv_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.srv_diff_host_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_count, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_count, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_same_srv_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_diff_srv_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_same_src_port_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_diff_host_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_serror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_serror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_rerror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_rerror_rate, DoubleType, nullable = true)
    .add(Enums.TransactionKafka.xattack, DoubleType, nullable = true)

  /* Transaction  Schema used while importing transaction data to Cassandra*/
  val fruadCheckedTransactionSchema: StructType = transactionSchema.add(Enums.TransactionKafka.xattack, DoubleType, nullable = true)


  /* Schema of transaction msgs received from Kafka. Json msg is received from Kafka. Hence evey field is treated as String */
  val kafkaTransactionStructureName: String = transactionStructureName
  val kafkaTransactionSchema: StructType = new StructType()
    .add(Enums.TransactionKafka.id, StringType, nullable = true)
    .add(Enums.TransactionKafka.trans_time, TimestampType, nullable = true)
    .add(Enums.TransactionKafka.duration, StringType, nullable = true)
    .add(Enums.TransactionKafka.protocol_type, StringType, nullable = true)
    .add(Enums.TransactionKafka.service, StringType, nullable = true)
    .add(Enums.TransactionKafka.flag, StringType, nullable = true)
    .add(Enums.TransactionKafka.src_bytes, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_bytes, StringType, nullable = true)
    .add(Enums.TransactionKafka.land, StringType, nullable = true)
    .add(Enums.TransactionKafka.wrong_fragment, StringType, nullable = true)
    .add(Enums.TransactionKafka.urgent, StringType, nullable = true)
    .add(Enums.TransactionKafka.hot, StringType, nullable = true)
    .add(Enums.TransactionKafka.num_failed_logins, StringType, nullable = true)
    .add(Enums.TransactionKafka.logged_in, StringType, nullable = true)
    .add(Enums.TransactionKafka.num_compromised, StringType, nullable = true)
    .add(Enums.TransactionKafka.root_shell, StringType, nullable = true)
    .add(Enums.TransactionKafka.su_attempted, StringType, nullable = true)
    .add(Enums.TransactionKafka.num_root, StringType, nullable = true)
    .add(Enums.TransactionKafka.num_file_creations, StringType, nullable = true)
    .add(Enums.TransactionKafka.num_shells, StringType, nullable = true)
    .add(Enums.TransactionKafka.num_access_files, StringType, nullable = true)
    .add(Enums.TransactionKafka.num_outbound_cmds, StringType, nullable = true)
    .add(Enums.TransactionKafka.is_host_login, StringType, nullable = true)
    .add(Enums.TransactionKafka.is_guest_login, StringType, nullable = true)
    .add(Enums.TransactionKafka.count, StringType, nullable = true)
    .add(Enums.TransactionKafka.srv_count, StringType, nullable = true)
    .add(Enums.TransactionKafka.serror_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.srv_serror_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.rerror_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.srv_rerror_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.same_srv_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.diff_srv_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.srv_diff_host_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_count, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_count, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_same_srv_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_diff_srv_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_same_src_port_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_diff_host_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_serror_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_serror_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_rerror_rate, StringType, nullable = true)
    .add(Enums.TransactionKafka.dst_host_srv_rerror_rate, StringType, nullable = true)


}
