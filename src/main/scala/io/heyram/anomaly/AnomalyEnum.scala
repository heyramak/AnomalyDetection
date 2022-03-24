package io.heyram.anomaly



object  Enums {


  object TransactionKafka extends Enumeration {

    val trans_time = "timestamp"
    val duration = "duration"
    val protocol_type = "protocol_type"
    val service = "service"
    val flag = "flag"
    val src_bytes = "src_bytes"
    val dst_bytes = "dst_bytes"
    val land = "land"
    val wrong_fragment = "wrong_fragment"
    val urgent = "urgent"
    val hot = "hot"
    val num_failed_logins = "num_failed_logins"
    val logged_in = "logged_in"
    val num_compromised = "num_compromised"
    val root_shell = "root_shell"
    val su_attempted = "su_attempted"
    val num_root = "num_root"
    val num_file_creations = "num_file_creations"
    val num_shells = "num_shells"
    val num_access_files = "num_access_files"
    val num_outbound_cmds = "num_outbound_cmds"
    val is_host_login = "is_host_login"
    val is_guest_login = "is_guest_login"
    val count = "count"
    val srv_count = "srv_count"
    val serror_rate = "serror_rate"
    val srv_serror_rate = "srv_serror_rate"
    val rerror_rate = "rerror_rate"
    val srv_rerror_rate = "srv_rerror_rate"
    val same_srv_rate = "same_srv_rate"
    val diff_srv_rate = "diff_srv_rate"
    val srv_diff_host_rate = "srv_diff_host_rate"
    val dst_host_count = "dst_host_count"
    val dst_host_srv_count = "dst_host_srv_count"
    val dst_host_same_srv_rate = "dst_host_same_srv_rate"
    val dst_host_diff_srv_rate = "dst_host_diff_srv_rate"
    val dst_host_same_src_port_rate = "dst_host_same_src_port_rate"
    val dst_host_srv_diff_host_rate = "dst_host_srv_diff_host_rate"
    val dst_host_serror_rate = "dst_host_serror_rate"
    val dst_host_srv_serror_rate = "dst_host_srv_serror_rate"
    val dst_host_rerror_rate = "dst_host_rerror_rate"
    val dst_host_srv_rerror_rate = "dst_host_srv_rerror_rate"
    val xAttack = "xAttack"
    val kafka_partition = "partition"
    val kafka_offset = "offset"

  }



  val TransactionCassandra = TransactionKafka
}