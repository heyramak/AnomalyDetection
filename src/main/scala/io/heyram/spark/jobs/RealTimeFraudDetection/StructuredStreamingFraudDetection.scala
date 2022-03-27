package io.heyram.spark.jobs.RealTimeFraudDetection

import io.heyram.spark.{DataReader, GracefulShutdown, SparkConfig}
import io.heyram.cassandra.{CassandraConfig, CassandraDriver}
import io.heyram.config.Config
import io.heyram.kafka.KafkaSource
import io.heyram.spark.jobs.SparkJob
import org.apache.log4j.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object StructuredStreamingFraudDetection extends SparkJob("Structured Streaming Job to detect fraud transaction"){

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    Config.parseArgs(args)
    import sparkSession.implicits._



    /*Offset is read from checkpointing, hence reading offset and saving offset to /from Cassandra is not required*/
    //val (startingOption, partitionsAndOffsets) = CassandraDriver.readOffset(CassandraConfig.keyspace, CassandraConfig.kafkaOffsetTable)

    val rawStream = KafkaSource.readStream().q//(startingOption, partitionsAndOffsets)

    val transactionStream = rawStream
      .selectExpr("transaction.*", "partition", "offset")



    val processedTransactionDF = transactionStream
      .select($"duration",$"protocol_type",$"service",$"flag",$"src_bytes",
        $"dst_bytes",$"land",$"wrong_fragment",$"urgent",$"hot",$"num_failed_logins",
        $"logged_in",$"num_compromised",$"root_shell",$"su_attempted",$"num_root",
        $"num_file_creations",$"num_shells",$"num_access_files",$"num_outbound_cmds",
        $"is_host_login",$"is_guest_login",$"count",$"srv_count",$"serror_rate",
        $"srv_serror_rate",$"rerror_rate",$"srv_rerror_rate",$"same_srv_rate",
        $"diff_srv_rate",$"srv_diff_host_rate",$"dst_host_count",$"dst_host_srv_count",
        $"dst_host_same_srv_rate",$"dst_host_diff_srv_rate",$"dst_host_same_src_port_rate",
        $"dst_host_srv_diff_host_rate",$"dst_host_serror_rate",$"dst_host_srv_serror_rate",
        $"dst_host_rerror_rate",$"dst_host_srv_rerror_rate", $"partition", $"offset")


        val preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath)
        val featureTransactionDF = preprocessingModel.transform(processedTransactionDF)

        val randomForestModel = RandomForestClassificationModel.load(SparkConfig.modelPath)
        val predictionDF =  randomForestModel.transform(featureTransactionDF).withColumnRenamed("predictiom", "xattack")
        //predictionDF.cache

        val anomalyPredictionDF = predictionDF.filter($"prediction" =!= 5)
        anomalyPredictionDF.withColumn("id", monotonically_increasing_id())

        val normalPredictionDF = predictionDF.filter($"prediction" === 5)
        normalPredictionDF.withColumn("id", monotonically_increasing_id())


        /*Save anomaly transactions to fraud_transaction table*/
        val anomalyQuery = CassandraDriver.saveForeach(anomalyPredictionDF, CassandraConfig.keyspace, CassandraConfig.anomalyTable, "anomalyQuery", "append")

        /*Save normal transactions to non_fraud_transaction table*/
        val normalQuery = CassandraDriver.saveForeach(normalPredictionDF, CassandraConfig.keyspace, CassandraConfig.normalTable, "normalQuery", "append")

        /*Offset is read from checkpointing, hence reading offset and saving offset to /from Cassandra is not required*/
        /*val kafkaOffsetDF = predictionDF.select("partition", "offset").groupBy("partition").agg(max("offset") as "offset")
        val offsetQuery = CassandraDriver.saveForeach(kafkaOffsetDF, CassandraConfig.keyspace, CassandraConfig.kafkaOffsetTable, "offsetQuery", "update")*/

        GracefulShutdown.handleGracefulShutdown(1000, List(anomalyQuery, normalQuery))

  }
}
