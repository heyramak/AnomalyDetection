package io.heyram.spark.algorithms

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel,RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger



object Algorithms {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def randomForestClassifier(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession): RandomForestClassificationModel = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(700)
    val model = randomForestEstimator.fit(training)
    val transactionWithPrediction = model.transform(test)
    logger.info(s"total data count is" + transactionWithPrediction.count())
    logger.info("count of same label " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
  def naiveBayes(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession): NaiveBayesModel = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val naiveBayesEstimator = new NaiveBayes().setLabelCol("label").setFeaturesCol("features").setModelType("multinomial")
    val model = naiveBayesEstimator.fit(training)
    val transactionWithPrediction = model.transform(test)
    logger.info(s"total data count is" + transactionWithPrediction.count())
    logger.info("count of same label " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
}
