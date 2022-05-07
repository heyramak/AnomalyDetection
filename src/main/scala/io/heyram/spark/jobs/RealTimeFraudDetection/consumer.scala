package io.heyram.spark.jobs.RealTimeFraudDetection

import io.heyram.spark.SparkConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel

import java.util.Properties
import scala.collection.JavaConverters._
object consumer extends App {
  val preprocessingModel = PipelineModel.load(SparkConfig.preprocessingModelPath)
  val randomForestModel = RandomForestClassificationModel.load(SparkConfig.randomForestModelPath)
  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("intrusionDetection")
  try {
    consumer.subscribe(topics.asJava)
    //consumer.subscribe(Collections.singletonList("topic_partition"))
    //consumer.subscribe(Pattern.compile("topic_partition"))
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value() +
          ", Offset: " + record.offset() + ", Partition: " + record.partition())

      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
}