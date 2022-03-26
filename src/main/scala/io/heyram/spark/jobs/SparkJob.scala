package io.heyram.spark.jobs

import io.heyram.spark.SparkConfig
import org.apache.spark.sql.SparkSession


abstract class SparkJob(appName:String) {


  lazy implicit val sparkSession: SparkSession = SparkSession.builder
    .config(SparkConfig.sparkConf)
    .getOrCreate()

}