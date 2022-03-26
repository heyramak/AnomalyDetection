package io.heyram.kafka

import io.heyram.config.Config
import org.apache.log4j.Logger

import scala.collection.mutable



object KafkaConfig {

  val logger: Logger = Logger.getLogger(getClass.getName)

  val kafkaParams: mutable.Map[String, String] = mutable.Map.empty

  /*Configuration setting are loaded from application.conf when you run Spark Standalone cluster*/
  def load(): Option[String] = {
    logger.info("Loading Kafka Setttings")
    kafkaParams.put("topic", Config.applicationConf.getString("config.kafka.topic"))
    kafkaParams.put("enable.auto.commit", Config.applicationConf.getString("config.kafka.enable.auto.commit"))
    kafkaParams.put("group.id", Config.applicationConf.getString("config.kafka.group.id"))
    kafkaParams.put("bootstrap.servers", Config.applicationConf.getString("config.kafka.bootstrap.servers"))
    kafkaParams.put("auto.offset.reset", Config.applicationConf.getString("config.kafka.auto.offset.reset"))
  }

  /* Default Settings will be used when you run the project from Intellij */
  def defaultSetting(): Option[String] = {

    kafkaParams.put("topic", "intrusionDetection")
    kafkaParams.put("enable.auto.commit", "false")
    kafkaParams.put("group.id", "RealTime Intrusion Detection")
    kafkaParams.put("bootstrap.servers", "localhost:9092")
    kafkaParams.put("auto.offset.reset", "earliest")
  }

}
