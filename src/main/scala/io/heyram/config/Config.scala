package io.heyram.config

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import io.heyram.cassandra.CassandraConfig
import io.heyram.kafka.KafkaConfig
import io.heyram.spark.SparkConfig
import org.apache.log4j.Logger


object Config {
  val logger: Logger = Logger.getLogger(getClass.getName)

  var applicationConf: Config = _


  var runMode = "local"
  var localProjectDir = ""

  /**
   * Parse a config object from application.conf file in src/main/resources
   * @param args
   * @return
   */
  def parseArgs(args: Array[String]): Unit = {

    if(args.length == 0) {
      defaultSettiing()
    } else {
      applicationConf = ConfigFactory.parseFile(new File(args(0)))
      val runMode = applicationConf.getString("config.mode")
      if(runMode == "local"){
        localProjectDir = s"file:///${System.getProperty("user.home")}/anomalydetection/"
      }
      loadConfig()
    }

  }

  def loadConfig(): Unit = {

    CassandraConfig.load()
    KafkaConfig.load()
    SparkConfig.load()
  }

  def defaultSettiing(): Unit = {

    CassandraConfig.defaultSettng()
    KafkaConfig.defaultSetting()
    SparkConfig.defaultSetting()
  }
}
