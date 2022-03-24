package io.heyram.cassandra

import io.heyram.config.Config
import org.apache.log4j.Logger


object CassandraConfig {

  val logger = Logger.getLogger(getClass.getName)

  var keyspace:String = _
  var anomalyTable:String = _
  var normalTable:String = _
  var kafkaOffsetTable:String = _
  var cassandrHost:String = _

  /*Configuration setting are loaded from application.conf when you run Spark Standalone cluster*/
  def load() = {
    logger.info("Loading Cassandra Setttings")
    keyspace = Config.applicationConf.getString("config.cassandra.keyspace")
    anomalyTable = Config.applicationConf.getString("config.cassandra.table.anomaly")
    normalTable = Config.applicationConf.getString("config.cassandra.table.normal")
    kafkaOffsetTable = Config.applicationConf.getString("config.cassandra.table.kafka.offset")
    cassandrHost = Config.applicationConf.getString("config.cassandra.host")

  }

  /* Default Settings will be used when you run the project from Intellij */
  def defaultSettng() = {
    keyspace = "intrusionDetection"
    anomalyTable = "anomaly"
    normalTable = "normal"
    kafkaOffsetTable = "kafka_offset"
    cassandrHost = "localhost"
  }
}
