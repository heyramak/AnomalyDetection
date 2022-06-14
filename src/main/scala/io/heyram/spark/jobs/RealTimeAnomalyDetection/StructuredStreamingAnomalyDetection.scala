package io.heyram.spark.jobs.RealTimeAnomalyDetection

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
import org.apache.spark.streaming.{Duration, StreamingContext}


object StructuredStreamingAnomalyDetection extends SparkJob("Structured Streaming Job to detect anomaly transaction"){

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    Config.parseArgs(args)
    import sparkSession.implicits._
    val ssc = new StreamingContext(sparkSession.sparkContext, Duration(SparkConfig.batchInterval))


    /*Offset is read from checkpointing, hence reading offset and saving offset to /from Cassandra is not required*/
    //val (startingOption, partitionsAndOffsets) = CassandraDriver.readOffset(CassandraConfig.keyspace, CassandraConfig.kafkaOffsetTable)

    val rawStream = KafkaSource.readStream()//(startingOption, partitionsAndOffsets)

    val transactionStream = rawStream
      .selectExpr("transaction.*")
    transactionStream.printSchema()




    val processedTransactionDF = transactionStream
      .select($"id",$"duration",$"protocol_type",$"service",$"flag",$"src_bytes",
        $"dst_bytes",$"land",$"wrong_fragment",$"urgent",$"hot",$"num_failed_logins",
        $"logged_in",$"num_compromised",$"root_shell",$"su_attempted",$"num_root",
        $"num_file_creations",$"num_shells",$"num_access_files",$"num_outbound_cmds",
        $"is_host_login",$"is_guest_login",$"count",$"srv_count",$"serror_rate",
        $"srv_serror_rate",$"rerror_rate",$"srv_rerror_rate",$"same_srv_rate",
        $"diff_srv_rate",$"srv_diff_host_rate",$"dst_host_count",$"dst_host_srv_count",
        $"dst_host_same_srv_rate",$"dst_host_diff_srv_rate",$"dst_host_same_src_port_rate",
        $"dst_host_srv_diff_host_rate",$"dst_host_serror_rate",$"dst_host_srv_serror_rate",
        $"dst_host_rerror_rate",$"dst_host_srv_rerror_rate")
      .withColumn("duration", lit($"duration") cast DoubleType)
      .withColumn("protocol_type", lit($"protocol_type") cast DoubleType)
      .withColumn("service", lit($"service") cast DoubleType)
      .withColumn("flag", lit($"flag") cast DoubleType)
      .withColumn("src_bytes", lit($"src_bytes") cast DoubleType)
      .withColumn("dst_bytes", lit($"dst_bytes") cast DoubleType)
      .withColumn("land", lit($"land") cast DoubleType)
      .withColumn("wrong_fragment", lit($"wrong_fragment") cast DoubleType)
      .withColumn("urgent", lit($"urgent") cast DoubleType)
      .withColumn("hot", lit($"hot") cast DoubleType)
      .withColumn("num_failed_logins", lit($"num_failed_logins") cast DoubleType)
      .withColumn("logged_in",lit($"logged_in") cast DoubleType)
      .withColumn("num_compromised", lit($"num_compromised") cast DoubleType)
      .withColumn("root_shell", lit($"root_shell") cast DoubleType)
      .withColumn("su_attempted", lit($"su_attempted") cast DoubleType)
      .withColumn("num_root", lit($"num_root") cast DoubleType)
      .withColumn("num_file_creations", lit($"num_file_creations") cast DoubleType)
      .withColumn("num_shells", lit($"num_shells") cast DoubleType)
      .withColumn("num_access_files", lit($"num_access_files") cast DoubleType)
      .withColumn("num_outbound_cmds", lit($"num_outbound_cmds") cast DoubleType)
      .withColumn("is_host_login", lit($"is_host_login") cast DoubleType)
      .withColumn("is_guest_login", lit($"is_guest_login") cast DoubleType)
      .withColumn("count", lit($"count") cast DoubleType)
      .withColumn("srv_count", lit($"srv_count") cast DoubleType)
      .withColumn("serror_rate", lit($"serror_rate") cast DoubleType)
      .withColumn("srv_serror_rate", lit($"srv_serror_rate") cast DoubleType)
      .withColumn("rerror_rate", lit($"rerror_rate") cast DoubleType)
      .withColumn("srv_rerror_rate", lit($"srv_rerror_rate") cast DoubleType)
      .withColumn("same_srv_rate", lit($"same_srv_rate") cast DoubleType)
      .withColumn("diff_srv_rate", lit($"diff_srv_rate") cast DoubleType)
      .withColumn("srv_diff_host_rate", lit($"srv_diff_host_rate") cast DoubleType)
      .withColumn("dst_host_count", lit($"dst_host_count") cast DoubleType)
      .withColumn("dst_host_srv_count", lit($"dst_host_srv_count") cast DoubleType)
      .withColumn("dst_host_same_srv_rate", lit($"dst_host_same_srv_rate") cast DoubleType)
      .withColumn("dst_host_diff_srv_rate", lit($"dst_host_diff_srv_rate") cast DoubleType)
      .withColumn("dst_host_same_src_port_rate", lit($"dst_host_same_src_port_rate") cast DoubleType)
      .withColumn("dst_host_srv_diff_host_rate", lit($"dst_host_srv_diff_host_rate") cast DoubleType)
      .withColumn("dst_host_serror_rate", lit($"dst_host_serror_rate") cast DoubleType)
      .withColumn("dst_host_srv_serror_rate", lit($"dst_host_srv_serror_rate") cast DoubleType)
      .withColumn("dst_host_rerror_rate", lit($"dst_host_rerror_rate") cast DoubleType)
      .withColumn("dst_host_srv_rerror_rate", lit($"dst_host_srv_rerror_rate") cast DoubleType)



        val preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath)
        val featureTransactionDF = preprocessingModel.transform(processedTransactionDF)

        val randomForestModel = RandomForestClassificationModel.load(SparkConfig.randomForestModelPath)
        val predictionDF =  randomForestModel.transform(featureTransactionDF)
        predictionDF.cache
        //predictionDF.printSchema()
        val anomalyPredictionDF = predictionDF.filter($"prediction" =!= 5.0)
        anomalyPredictionDF.withColumnRenamed("prediction","xattack")


        val normalPredictionDF = predictionDF.filter($"prediction" === 5.0)
        normalPredictionDF.withColumnRenamed("prediction","xattack")
        logger.info("Crossed 1")


        /*Save anomaly transactions to anomaly table*/
        val anomalyQuery = CassandraDriver.saveForeach(anomalyPredictionDF, CassandraConfig.keyspace, CassandraConfig.anomalyTable, "anomalyQuery", "append")

        /*Save normal transactions to normal table*/
        val normalQuery = CassandraDriver.saveForeach(normalPredictionDF, CassandraConfig.keyspace, CassandraConfig.normalTable, "normalQuery", "append")

        /*Offset is read from checkpointing, hence reading offset and saving offset to /from Cassandra is not required*/
        /*val kafkaOffsetDF = predictionDF.select("partition", "offset").groupBy("partition").agg(max("offset") as "offset")
        val offsetQuery = CassandraDriver.saveForeach(kafkaOffsetDF, CassandraConfig.keyspace, CassandraConfig.kafkaOffsetTable, "offsetQuery", "update")*/

        GracefulShutdown.handleGracefulShutdown(1000,  List(anomalyQuery, normalQuery))

  }
}
