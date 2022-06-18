package io.heyram.spark.algorithms

import org.apache.spark.ml.classification._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator



object Algorithms {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def randomForestClassifier(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession): RandomForestClassificationModel = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxBins(700)
    val model = randomForestEstimator.fit(training)
    val transactionWithPrediction = model.transform(test)
    val predictionAndLabels = transactionWithPrediction.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
    println("Accuracy = " + evaluator.setMetricName("accuracy").evaluate(predictionAndLabels)
      + "\nPrecision = " + evaluator.setMetricName("weightedPrecision").evaluate(predictionAndLabels)
      + "\nF1 = " + evaluator.setMetricName("f1").evaluate(predictionAndLabels)
      + "\nTotal data count is " + transactionWithPrediction.count()
      + "\nCount of same label is " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
  def naiveBayes(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession): NaiveBayesModel = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val naiveBayesEstimator = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setModelType("multinomial")
    val model = naiveBayesEstimator.fit(training)
    val transactionWithPrediction = model.transform(test)
    val predictionAndLabels = transactionWithPrediction.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
    println("Accuracy = " + evaluator.setMetricName("accuracy").evaluate(predictionAndLabels)
      + "\nPrecision = " + evaluator.setMetricName("weightedPrecision").evaluate(predictionAndLabels)
      + "\nF1 = " + evaluator.setMetricName("f1").evaluate(predictionAndLabels)
      + "\nTotal data count is " + transactionWithPrediction.count()
      + "\nCount of same label is " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
  def logisticRegression(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession): LogisticRegressionModel = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val logisticRegressionEstimator = new LogisticRegression()
      .setMaxIter(20)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setLabelCol("label")
      .setFeaturesCol("features")
    val model = logisticRegressionEstimator.fit(training)
    val transactionWithPrediction = model.transform(test)
    val predictionAndLabels = transactionWithPrediction.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
    println(s"Coefficients: \n${model.coefficientMatrix}"
      + s"\nIntercepts: ${model.interceptVector}"
      + "\nAccuracy = " + evaluator.setMetricName("accuracy").evaluate(predictionAndLabels)
      + "\nPrecision = " + evaluator.setMetricName("weightedPrecision").evaluate(predictionAndLabels)
      + "\nF1 = " + evaluator.setMetricName("f1").evaluate(predictionAndLabels)
      + "\nTotal data count is " + transactionWithPrediction.count()
      + "\nCount of same label is " + transactionWithPrediction.filter($"prediction" === $"label").count())
    model
  }
}
