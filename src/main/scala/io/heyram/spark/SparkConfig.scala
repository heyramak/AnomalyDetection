package io.heyram.spark

import io.heyram.cassandra.CassandraConfig
import io.heyram.config.Config
import org.apache.log4j.Logger
import org.apache.spark.SparkConf


object SparkConfig {
   val logger: Logger = Logger.getLogger(getClass.getName)

   val sparkConf = new SparkConf

   var trainingDatasource:String = _
   var attackTypes:String = _
   var logisticRegressionModelPath:String=_
   var randomForestModelPath:String = _
   var randomForestWithoutKMeansModelPath:String = _
   var naiveBayesModelPath:String= _
   var naiveBayesWithoutKMeansModelPath:String= _
   var preprocessingModelPath:String = _
   var shutdownMarker:String = _
   var batchInterval:Int = _


    def load(): Unit = {
      logger.info("Loading Spark Setttings")
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", Config.applicationConf.getString("config.spark.gracefulShutdown"))
        .set("spark.sql.streaming.checkpointLocation", Config.applicationConf.getString("config.spark.checkpoint"))
        .set("spark.cassandra.connection.host", Config.applicationConf.getString("config.cassandra.host"))
      shutdownMarker = Config.applicationConf.getString("config.spark.shutdownPath")
      batchInterval = Config.applicationConf.getString("config.spark.batch.interval").toInt
      trainingDatasource = Config.localProjectDir + Config.applicationConf.getString("config.spark.training.datasource")
      attackTypes = Config.localProjectDir + Config.applicationConf.getString("config.spark.attack.types")
      randomForestModelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.random-forest.path")
      randomForestWithoutKMeansModelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.random-forest-without-kmeans.path")
      naiveBayesModelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.naive-bayes.path")
      naiveBayesWithoutKMeansModelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.naive-bayes-without-kmeans.path")
      preprocessingModelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.preprocessing.path")
      logisticRegressionModelPath = Config.localProjectDir + Config.applicationConf.getString("config.spark.model.logistic-regression.path")
    }

    def defaultSetting(): Unit = {
      sparkConf.setMaster("local[*]")
        .set("spark.cassandra.connection.host", CassandraConfig.cassandrHost)
        .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
      shutdownMarker = "/tmp/shutdownmarker"
      trainingDatasource ="src/main/resources/data/training.csv"
      attackTypes="src/main/resources/attacks.json"
      logisticRegressionModelPath="src/main/resources/spark/training/LogisticRegressionModel"
      randomForestModelPath = "src/main/resources/spark/training/RandomForestModel"
      randomForestWithoutKMeansModelPath = "src/main/resources/spark/training/RandomForestWithoutKMeansModel"
      naiveBayesModelPath = "src/main/resources/spark/training/NaiveBayesModel"
      naiveBayesWithoutKMeansModelPath = "src/main/resources/spark/training/NaiveBayesWithoutKMeansModel"
      preprocessingModelPath = "src/main/resources/spark/training/PreprocessingModel"
      batchInterval = 5000

    }
 }
