#!/usr/bin/env bash
# Import intial data to Cassandra..
spark-submit  --class io.heyram.spark.jobs.ImportToCassandra --name "Import to Cassandra" --master spark://heyram:7077 $HOME/anomalydetection/spark/anomalydetection-spark.jar $HOME/anomalydetection/spark/application-local.conf