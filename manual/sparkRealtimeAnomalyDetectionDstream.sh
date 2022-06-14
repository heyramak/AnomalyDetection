#!/usr/bin/env bash
#Dstream Streaming Spark Job
spark-submit --class io.heyram.spark.jobs.RealTimeAnomalyDetection.DstreamAnomalyDetection --name "Anomaly Detection using Dstream" --master spark://heyram:6066 --deploy-mode cluster  $HOME/anomalydetection/spark/anomalydetection-spark.jar $HOME/anomalydetection/spark/application-local.conf