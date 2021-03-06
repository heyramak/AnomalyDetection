package io.heyram.cassandra

import java.io.{BufferedReader, FileReader}

import com.datastax.spark.connector.cql.CassandraConnector
import com.google.gson.{Gson, JsonObject}
import io.heyram.cassandra.foreachSink.CassandraSinkForeach
import io.heyram.spark.SparkConfig
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQuery


object CassandraDriver {

  val logger: Logger = Logger.getLogger(getClass.getName)

  val connector: CassandraConnector = CassandraConnector(SparkConfig.sparkConf)


  def debugStream(ds: Dataset[_], mode: String = "append"): StreamingQuery = {

    ds.writeStream
      .format("console")
      .option("truncate", "false")
      .option("numRows", "100")
      .outputMode(mode)
      .start()
  }


  def saveForeach(df: DataFrame, db:String, table:String , queryName: String, mode:String): StreamingQuery = {

    println("Calling saveForeach")
    df
      .writeStream
      .queryName(queryName)
      .outputMode(mode)
      .foreach(new CassandraSinkForeach(db, table))
      .start()
  }


  /* Read and prepare offset for Structrued Streaming */
  def readOffset(keyspace:String, table:String)(implicit sparkSession:SparkSession): (String, String) = {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace",keyspace)
      .option("table",table)
      .option("pushdown", "true")
      .load()
      .select("partition", "offset")
      //.filter($"partition".isNotNull)
    //df.show(false)

    if( df.rdd.isEmpty()) {
      ("startingOffsets", "earliest")
    }
    else {
      /*
      val offsetDf = df.select("partition", "offset")
        .groupBy("partition").agg(max("offset") as "offset")
      ("startingOffsets", transformKafkaMetadataArrayToJson(offsetDf.collect()))
      */
      ("startingOffsets", transformKafkaMetadataArrayToJson(df.collect()))
    }
  }

  /**
   * @param array
   * @return {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
   */
  def transformKafkaMetadataArrayToJson(array: Array[Row]): String = {

    val partitionOffset = array
      .toList
      .foldLeft("")((a, i) => {
        a + s""""${i.getAs[Int](("partition"))}":${i.getAs[Long](("offset"))}, """
      })

    println("Offset: " + partitionOffset.substring(0, partitionOffset.length -2))

    val partitionAndOffset  = s"""{"anomalyDetection":
          {
           ${partitionOffset.substring(0, partitionOffset.length -2)}
          }
         }
      """.replaceAll("\n", "").replaceAll(" ", "")
    println(partitionAndOffset)
    partitionAndOffset
  }



  /* Read offsert from Cassandra for Dstream*/
  def readOffset(keySpace:String, table:String, topic:String)(implicit sparkSession:SparkSession): Option[Map[TopicPartition, Long]] = {
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace",keySpace)
      .option("table",table)
      .option("pushdown", "true")
      .load()
      .select("partition", "offset")

    if (df.rdd.isEmpty()) {
      logger.info("No offset. Read from earliest")
      None
    }
    else {
      val fromOffsets = df.rdd.collect().map(o => {
        println(o)
        (new TopicPartition(topic, o.getAs[Int]("partition")), o.getAs[Long]("offset"))
      }
      ).toMap
      Some(fromOffsets)
    }
  }


  /* Save Offset to Cassandra for Structured Streaming */
  def saveOffset(keySpace:String, table:String, df:DataFrame)(implicit sparkSession:SparkSession): Unit = {

    df.write
     .format("org.apache.spark.sql.cassandra")
     .options(Map("keyspace" -> keySpace, "table" -> table))
     .save()
  }

 }
