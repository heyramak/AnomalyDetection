package io.heyram.spark.jobs.RealTimeAnomalyDetection

import java.io.FileReader

import com.datastax.spark.connector.cql.CassandraConnector
import io.heyram.cassandra.dao.{IntrusionDetectionRepository, KafkaOffsetRepository}
import io.heyram.cassandra.{CassandraConfig, CassandraDriver}
import io.heyram.config.Config
import io.heyram.anomaly.Schema
import io.heyram.kafka.KafkaConfig
import io.heyram.spark.{GracefulShutdown, SparkConfig}
import io.heyram.spark.jobs.SparkJob
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{NaiveBayesModel, RandomForestClassificationModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import com.flyberrycapital.slack.SlackClient
import com.google.gson.Gson
import com.google.gson.JsonObject
import java.io.BufferedReader

import scala.collection.mutable



object DstreamAnomalyDetection extends SparkJob("Anomaly Detection using Dstream"){

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main (args: Array[String]): Unit = {

    Config.parseArgs(args)

    import sparkSession.implicits._

    SparkSession
      .builder()
      .appName("Anomaly Detection using Dstream")
      .config("spark.master", "local[*]")
      .getOrCreate()

    /* Load Preprocessing Model and Random Forest Model saved by Spark ML Job i.e AnomalyDetectionTraining */
    val preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath)
    val randomForestModel = RandomForestClassificationModel.load(SparkConfig.randomForestWithoutKMeansModelPath)
    //val naiveBayesModel = NaiveBayesModel.load(SparkConfig.naiveBayesModelPath)

    /*
       Connector Object is created in driver. It is serializable.
       So once the executor get it, they establish the real connection
    */
    val connector = CassandraConnector(sparkSession.sparkContext.getConf)

    val brodcastMap = sparkSession.sparkContext.broadcast(mutable.Map("keyspace" -> CassandraConfig.keyspace,
      "anomalyTable" -> CassandraConfig.anomalyTable,
      "normalTable" -> CassandraConfig.normalTable,
      "kafkaOffsetTable" -> CassandraConfig.kafkaOffsetTable))


    val ssc = new StreamingContext(sparkSession.sparkContext, Duration(SparkConfig.batchInterval))



    val topics = Set(KafkaConfig.kafkaParams("topic"))
    val kafkaParams = mutable.Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> KafkaConfig.kafkaParams("bootstrap.servers"),
      ConsumerConfig.GROUP_ID_CONFIG -> KafkaConfig.kafkaParams("group.id"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> KafkaConfig.kafkaParams("auto.offset.reset")
    )


    val storedOffsets = CassandraDriver.readOffset(CassandraConfig.keyspace,
                           CassandraConfig.kafkaOffsetTable, KafkaConfig.kafkaParams("topic"))

    val stream = storedOffsets match {
      case None =>
        KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Subscribe[String, String](topics, kafkaParams)
                       )


      case Some(fromOffsets) =>
        KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))

    }

    val transactionStream =  stream.map(cr => (cr.value(), cr.partition(), cr.offset()))

    transactionStream.foreachRDD(rdd => {

      if (!rdd.isEmpty()) {

        val kafkaTransactionDF = rdd.toDF("transaction", "partition", "offset")
          .withColumn(Schema.kafkaTransactionStructureName, // nested structure with our json
            from_json($"transaction", Schema.kafkaTransactionSchema)) //From binary to JSON object
          .select("transaction.*", "partition", "offset")
          .withColumn("duration", lit($"duration") cast DoubleType)
          //.withColumn("protocol_type", lit($"protocol_type") cast DoubleType)
          //.withColumn("service", lit($"service") cast DoubleType)
          //.withColumn("flag", lit($"flag") cast DoubleType)
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
          .withColumn("trans_time", expr("reflect('java.time.LocalDateTime', 'now')") cast TimestampType)


        //kafkaTransactionDF.show


        sparkSession.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")



        val featureTransactionDF = preprocessingModel.transform(kafkaTransactionDF)
        val predictionDF = randomForestModel.transform(featureTransactionDF)
          .withColumnRenamed("prediction", "xattack")
        logger.info("Crossed modelling phase")
        predictionDF.show


        /*
         Connector Object is created in driver. It is serializable.
         It is serialized and send to executor. Once the executor get it, they establish the real connection
        */

        predictionDF.foreachPartition(partitionOfRecords => {

          /*
          * dbname and table name are initialzed in the driver. foreachPartition is called in the executor, hence dbname
          * and table names have to be broadcasted
          */
          val keyspace = brodcastMap.value("keyspace")
          val anomalyTable = brodcastMap.value("anomalyTable")
          val normalTable = brodcastMap.value("normalTable")
          val kafkaOffsetTable = brodcastMap.value("kafkaOffsetTable")

 /*
          Writing to Anomaly, Normal and Offset Table in single iteration
          Cassandra prepare statement is used because it avoids pasring of the column for every insert and hence efficient
          Offset is inserted last to achieve atleast once semantics.
*/

          connector.withSessionDo(session => {
            //Prepare Statement for all three tables
            val preparedStatementAnomaly = session.prepare(IntrusionDetectionRepository.cqlTransactionPrepare(keyspace, anomalyTable))
            val preparedStatementNormal = session.prepare(IntrusionDetectionRepository.cqlTransactionPrepare(keyspace, normalTable))
            val preparedStatementOffset = session.prepare(KafkaOffsetRepository.cqlOffsetPrepare(keyspace, kafkaOffsetTable))

            val partitionOffset:mutable.Map[Int, Long] = mutable.Map.empty
            partitionOfRecords.foreach(record => {
              val xattack = record.getAs[Double]("xattack")
              val id = record.getAs[String]("id")
              val path = SparkConfig.attackTypes
              val bufferedReader = new BufferedReader(new FileReader(path))
              val gson = new Gson
              val js = gson.fromJson(bufferedReader, classOf[JsonObject])
              val attacktype = js.get(xattack.toString()).getAsString
              if (xattack != 0.0) {
                // Bind and execute prepared statement for Anomaly Table
                val s = new SlackClient("xoxb-3653430754599-3667220974597-8cD0yBWV9Co99ZdAnlOZozQC")
                val text = "Id: " + id + "\n" + "Attack type: " + attacktype
                s.chat.postMessage("#detecting-anomalies", text)
                session.execute(IntrusionDetectionRepository.cqlTransactionBind(preparedStatementAnomaly, record))
              }
              else if(xattack == 0.0) {
                // Bind and execute prepared statement for Normal Table
                session.execute(IntrusionDetectionRepository.cqlTransactionBind(preparedStatementNormal, record))
              }
              //Get max offset in the current match
              val kafkaPartition = record.getAs[Int]("partition")
              val offset = record.getAs[Long]("offset")
              partitionOffset.get(kafkaPartition) match  {
                case None => partitionOffset.put(kafkaPartition, offset)
                case Some(currentMaxOffset) =>
                  if(offset > currentMaxOffset)
                    partitionOffset.update(kafkaPartition, offset)


              }

            })
            partitionOffset.foreach(t => {
              // Bind and execute prepared statement for Offset Table
              session.execute(KafkaOffsetRepository.cqlOffsetBind(preparedStatementOffset, t))

            })

          })
        })


      }
      else {
        logger.info("Did not receive any data")
      }

    })

    ssc.start()
    GracefulShutdown.handleGracefulShutdown(1000, ssc)
  }

}

