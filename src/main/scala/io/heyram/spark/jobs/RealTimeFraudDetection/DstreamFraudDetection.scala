package io.heyram.spark.jobs.RealTimeFraudDetection


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
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._

import scala.collection.mutable.Map


object DstreamFraudDetection extends SparkJob("Anomaly Detection using Dstream"){

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main (args: Array[String]): Unit = {

    Config.parseArgs(args)

    import sparkSession.implicits._
    SparkSession
      .builder()
      .appName("Real time anomaly detection")
      .config("spark.master", "local")
      .getOrCreate();
    /* Load Preprocessing Model and Random Forest Model saved by Spark ML Job i.e FraudDetectionTraining */
    val preprocessingModel = PipelineModel.load("/home/heyram/workspace/AnomalyDetection/src/main/resources/spark/training/PreprocessingModel")
    val randomForestModel = RandomForestClassificationModel.load("/home/heyram/workspace/AnomalyDetection/src/main/resources/spark/training/RandomForestModel")

    /*
       Connector Object is created in driver. It is serializable.
       So once the executor get it, they establish the real connection
    */
    val connector = CassandraConnector(sparkSession.sparkContext.getConf)

    val brodcastMap = sparkSession.sparkContext.broadcast(Map("keyspace" -> CassandraConfig.keyspace,
      "anomalyTable" -> CassandraConfig.anomalyTable,
      "normalTable" -> CassandraConfig.normalTable,
      "kafkaOffsetTable" -> CassandraConfig.kafkaOffsetTable))


    val ssc = new StreamingContext(sparkSession.sparkContext, Duration(SparkConfig.batchInterval))


    val topics = Set(KafkaConfig.kafkaParams("topic"))
    val kafkaParams = Map[String, String](
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
      case None => {
        KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Subscribe[String, String](topics, kafkaParams)
                       )
      }

      case Some(fromOffsets) => {
        KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
      }
    }

    val transactionStream =  stream.map(cr => (cr.value(), cr.partition(), cr.offset()))

    transactionStream.foreachRDD(rdd => {

      if (!rdd.isEmpty()) {

        val kafkaTransactionDF = rdd.toDF("transaction", "partition", "offset")
          .withColumn(Schema.kafkaTransactionStructureName, // nested structure with our json
            from_json($"transaction", Schema.kafkaTransactionSchema)) //From binary to JSON object
          .select("transaction.*", "partition", "offset")


        sparkSession.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")


        val featureTransactionDF = preprocessingModel.transform(kafkaTransactionDF)
        val predictionDF = randomForestModel.transform(featureTransactionDF)
          .withColumnRenamed("prediction", "xattack")

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
          Writing to Fraud, NonFruad and Offset Table in single iteration
          Cassandra prepare statement is used because it avoids pasring of the column for every insert and hence efficient
          Offset is inserted last to achieve atleast once semantics. it is possible that it may read duplicate creditcard
          transactions from kafka while restart.
          Even though duplicate creditcard transaction are read from kafka, writing to Cassandra is idempotent. Becasue
          cc_num and trans_time is the primary key. So you cannot have duplicate records with same cc_num and trans_time.
          As a result we achive exactly once semantics.
*/

          connector.withSessionDo(session => {
            //Prepare Statement for all three tables
            val preparedStatementAnomaly = session.prepare(IntrusionDetectionRepository.cqlTransactionPrepare(keyspace, anomalyTable))
            val preparedStatementNormal = session.prepare(IntrusionDetectionRepository.cqlTransactionPrepare(keyspace, normalTable))
            val preparedStatementOffset = session.prepare(KafkaOffsetRepository.cqlOffsetPrepare(keyspace, kafkaOffsetTable))

            val partitionOffset:Map[Int, Long] = Map.empty
            partitionOfRecords.foreach(record => {
              val xattack = record.getAs[Double]("xattack")
              if (xattack != 5.0) {
                // Bind and execute prepared statement for Fraud Table
                session.execute(IntrusionDetectionRepository.cqlTransactionBind(preparedStatementAnomaly, record))
              }
              else if(xattack == 5.0) {
                // Bind and execute prepared statement for NonFraud Table
                session.execute(IntrusionDetectionRepository.cqlTransactionBind(preparedStatementNormal, record))
              }
              //Get max offset in the current match
              val kafkaPartition = record.getAs[Int]("partition")
              val offset = record.getAs[Long]("offset")
              partitionOffset.get(kafkaPartition) match  {
                case None => partitionOffset.put(kafkaPartition, offset)
                case Some(currentMaxOffset) => {
                  if(offset > currentMaxOffset)
                    partitionOffset.update(kafkaPartition, offset)
                }

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
