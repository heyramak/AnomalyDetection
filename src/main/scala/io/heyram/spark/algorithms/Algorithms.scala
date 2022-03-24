package io.heyram.spark.algorithms

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger


object Algorithms {

  val logger = Logger.getLogger(getClass.getName)

  def randomForestClassifier(df: org.apache.spark.sql.DataFrame)(implicit sparkSession:SparkSession) = {
    import sparkSession.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(700)
    val model = randomForestEstimator.fit(training)
    val transactionwithPrediction = model.transform(test)
    logger.info(s"total data count is" + transactionwithPrediction.count())
    logger.info("count of same label " + transactionwithPrediction.filter($"prediction" === $"label").count())
    model
  }
}
