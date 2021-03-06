package io.heyram.spark

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka010.HasOffsetRanges


object DataReader {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def read(trainingDatasource:String, schema:StructType)(implicit sparkSession:SparkSession): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .schema(schema)
      .csv(trainingDatasource)
  }

  def readFromCassandra(keySpace:String, table:String)(implicit sparkSession:SparkSession): DataFrame = {

    sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> table, "pushdown" -> "true"))
      .load()
  }

  def getOffset(rdd: RDD[_])(implicit sparkSession:SparkSession): DataFrame = {

    import  sparkSession.implicits._
    rdd.asInstanceOf[HasOffsetRanges]
      .offsetRanges.toList
      .map(offset => (offset.partition, offset.untilOffset))
      .toDF("partition", "offset")
  }
}
