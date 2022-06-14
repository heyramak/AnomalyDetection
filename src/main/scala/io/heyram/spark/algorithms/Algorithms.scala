package io.heyram.spark.algorithms

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator



object Algorithms {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def randomForestClassifier(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession): RandomForestClassificationModel = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(700)
    val model = randomForestEstimator.fit(training)
    val transactionWithPrediction = model.transform(test)
    val predictionAndLabels = transactionWithPrediction.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
    println("Accuracy = " + evaluator.setMetricName("accuracy").evaluate(predictionAndLabels))
    println("Precision = " + evaluator.setMetricName("weightedPrecision").evaluate(predictionAndLabels))
    println("F1 = " + evaluator.setMetricName("f1").evaluate(predictionAndLabels))
    logger.info(s"Total data count is" + transactionWithPrediction.count())
    logger.info("Count of same label " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
  def naiveBayes(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession): NaiveBayesModel = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val naiveBayesEstimator = new NaiveBayes().setLabelCol("label").setFeaturesCol("features").setModelType("multinomial")
    val model = naiveBayesEstimator.fit(training)
    val transactionWithPrediction = model.transform(test)
    val predictionAndLabels = transactionWithPrediction.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
    println("Accuracy = " + evaluator.setMetricName("accuracy").evaluate(predictionAndLabels))
    println("Precision = " + evaluator.setMetricName("weightedPrecision").evaluate(predictionAndLabels))
    println("F1 = " + evaluator.setMetricName("f1").evaluate(predictionAndLabels))
    logger.info(s"Total data count is" + transactionWithPrediction.count())
    logger.info("Count of same label " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
}
