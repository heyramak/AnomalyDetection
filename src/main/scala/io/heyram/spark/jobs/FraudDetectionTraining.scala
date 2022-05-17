package io.heyram.spark.jobs

import io.heyram.spark.{DataBalancing, DataReader, SparkConfig}
import io.heyram.cassandra.CassandraConfig
import io.heyram.config.Config
import io.heyram.spark.algorithms.Algorithms
import io.heyram.spark.pipeline.BuildPipeline
import org.apache.spark.ml.Pipeline


object FraudDetectionTraining extends SparkJob("Balancing Fraud & Non-Fraud Dataset"){


  def main(args: Array[String]): Unit = {

    Config.parseArgs(args)

    import sparkSession.implicits._

    val anomalyDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.anomalyTable)
      .select("duration","protocol_type","service","flag","src_bytes",
        "dst_bytes","land","wrong_fragment","urgent","hot","num_failed_logins",
        "logged_in","num_compromised","root_shell","su_attempted","num_root",
        "num_file_creations","num_shells","num_access_files","num_outbound_cmds",
        "is_host_login","is_guest_login","count","srv_count","serror_rate",
        "srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate",
        "diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count",
        "dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate",
        "dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate",
        "dst_host_rerror_rate","dst_host_srv_rerror_rate","xAttack")

    val normalDF = DataReader.readFromCassandra(CassandraConfig.keyspace, CassandraConfig.normalTable)
      .select("duration","protocol_type","service","flag","src_bytes",
        "dst_bytes","land","wrong_fragment","urgent","hot","num_failed_logins",
        "logged_in","num_compromised","root_shell","su_attempted","num_root",
        "num_file_creations","num_shells","num_access_files","num_outbound_cmds",
        "is_host_login","is_guest_login","count","srv_count","serror_rate",
        "srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate",
        "diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count",
        "dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate",
        "dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate",
        "dst_host_rerror_rate","dst_host_srv_rerror_rate","xattack")

    val transactionDF = normalDF.union(anomalyDF)
    transactionDF.cache()


    val coloumnNames = List("duration","protocol_type","service","flag","src_bytes",
      "dst_bytes","land","wrong_fragment","urgent","hot","num_failed_logins",
      "logged_in","num_compromised","root_shell","su_attempted","num_root",
      "num_file_creations","num_shells","num_access_files","num_outbound_cmds",
      "is_host_login","is_guest_login","count","srv_count","serror_rate",
      "srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate",
      "diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count",
      "dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate",
      "dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate",
      "dst_host_rerror_rate","dst_host_srv_rerror_rate")

    val pipelineStages = BuildPipeline.createFeaturePipeline(transactionDF.schema, coloumnNames)
    val pipeline = new Pipeline().setStages(pipelineStages)
    val PreprocessingTransformerModel = pipeline.fit(transactionDF)
    PreprocessingTransformerModel.write.overwrite().save(SparkConfig.preprocessingModelPath)

    val featureDF = PreprocessingTransformerModel.transform(transactionDF)


    val fraudDF = featureDF
      .filter($"xattack" =!= 0.0)
      .withColumnRenamed("xattack", "label")
      .select("features", "label")

    val nonFraudDF = featureDF.filter($"xattack" === 0.0)
    val fraudCount = fraudDF.count()


    /* There will be very few fraud transaction and more normal transaction. Models created  from such
     * imbalanced data will not have good prediction accuracy. Hence balancing the dataset. K-means is used for balancing
     */
    val balancedNonFraudDF = DataBalancing.createBalancedDataframe(nonFraudDF, fraudCount.toInt)
    val finalfeatureDF = fraudDF.union(balancedNonFraudDF)


    val randomForestModel = Algorithms.randomForestClassifier(finalfeatureDF)
    randomForestModel.write.overwrite().save(SparkConfig.randomForestModelPath)

    val naiveBayesModel = Algorithms.naiveBayes(finalfeatureDF)
    naiveBayesModel.write.overwrite().save(SparkConfig.naiveBayesModelPath)

  }

}
