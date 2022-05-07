package io.heyram.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.streaming.StreamingContext


object GracefulShutdown {

  val logger: Logger = Logger.getLogger(getClass.getName)

  var stopFlag:Boolean = false

  def checkShutdownMarker(): Unit = {
    if (!stopFlag) {
      stopFlag =  new java.io.File(SparkConfig.shutdownMarker).exists()
    }

  }

  /* Handle Structured Streaming graceful shutdown. */
  def handleGracefulShutdown(checkIntervalMillis:Int, streamingQueries: List[StreamingQuery])(implicit sparkSession: SparkSession): Unit = {

    var isStopped = false

    while (! isStopped) {
      logger.info("calling awaitTerminationOrTimeout")
      isStopped = sparkSession.streams.awaitAnyTermination(checkIntervalMillis)
      if (isStopped)
        logger.info("confirmed! The streaming context is stopped. Exiting application...")
      else
        logger.info("Streaming App is still running. Timeout...")
      checkShutdownMarker()
      if (!isStopped && stopFlag) {
        logger.info("stopping ssc right now")
        streamingQueries.foreach(query => {
          query.stop()
        })
        sparkSession.stop
        logger.info("ssc is stopped!!!!!!!")
      }
    }
  }


  /* Handle Dstream graceful shutdown. */
  def handleGracefulShutdown(checkIntervalMillis:Int, ssc:StreamingContext)(implicit sparkSession: SparkSession): Unit = {

    var isStopped = false

    while (! isStopped) {
      logger.info("calling awaitTerminationOrTimeout")
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        logger.info("confirmed! The streaming context is stopped. Exiting application...")
      else
        logger.info("Streaming App is still running. Timeout...")
      checkShutdownMarker()
      if (!isStopped && stopFlag) {
        logger.info("stopping ssc right now")
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        sparkSession.stop
        logger.info("ssc is stopped!!!!!!!")
      }
    }

  }
}
