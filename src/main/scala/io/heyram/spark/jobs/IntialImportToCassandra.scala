package io.heyram.spark.jobs

import io.heyram.cassandra.CassandraDriver
import io.heyram.spark.SparkConfig
import io.heyram.cassandra.CassandraConfig
import io.heyram.config.Config
import io.heyram.anomaly.Schema
import io.heyram.spark.{DataReader, SparkConfig}
import org.apache.spark.sql.functions._
import java.time
import org.apache.spark.sql.types.{IntegerType, TimestampType}


object IntialImportToCassandra extends SparkJob("Initial Import to Cassandra"){

  def main(args: Array[String]) {

    Config.parseArgs(args)

    import sparkSession.implicits._

    val transactionDF = DataReader.read("/home/hduser/project/AnomalyDetection/src/main/resources/data/KDDTrain+.csv", Schema.transactionSchema)


    val processedDF = transactionDF
      .select("duration","protocol_type","service","flag","src_bytes",
        "dst_bytes","land","wrong_fragment","urgent","hot","num_failed_logins",
        "logged_in","num_compromised","root_shell","su_attempted","num_root",
        "num_file_creations","num_shells","num_access_files","num_outbound_cmds",
        "is_host_login","is_guest_login","count","srv_count","serror_rate",
        "srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate",
        "diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count",
        "dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate",
        "dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate",
        "dst_host_rerror_rate","dst_host_srv_rerror_rate","id")



    processedDF.cache()

    val anomalyDF = processedDF.filter($"xAttack" =!= 4)
    val normalDF = processedDF.filter($"xAttack" === 4)

    /* Save anomaly transaction data to anomaly cassandra table*/
    processedDF.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("keyspace" -> CassandraConfig.keyspace, "table" -> CassandraConfig.anomalyTable))
      .save()

    /* Save normal transaction data to normal cassandra table*/
    normalDF.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map("keyspace" -> CassandraConfig.keyspace, "table" -> CassandraConfig.normalTable))
      .save()


  }

}
