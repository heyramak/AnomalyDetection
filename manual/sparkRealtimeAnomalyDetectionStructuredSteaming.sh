#!/usr/bin/env bash
#Structured Streaming Spark Job
spark-submit --class io.heyram.spark.jobs.RealTimeAnomalyDetection.StructuredStreamingAnomalyDetection --name "Structured Streaming Job to detect anomaly transaction" --master spark://heyram:7077 --deploy-mode cluster  $HOME/anomalydetection/spark/anomalydetection-spark.jar $HOME/anomalydetection/spark/application-local.conf