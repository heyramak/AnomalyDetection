#!/usr/bin/env bash
#Training Spark Job
spark-submit --class io.heyram.spark.jobs.AnomalyDetectionTraining --name "Anomaly Detection ML Training" --master spark://heyram:7077 $HOME/anomalydetection/spark/anomalydetection-spark.jar  $HOME/anomalydetection/spark/application-local.conf