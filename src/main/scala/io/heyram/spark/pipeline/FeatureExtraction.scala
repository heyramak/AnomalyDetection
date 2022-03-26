package io.heyram.spark.pipeline

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.{StringIndexerModel, OneHotEncoder, VectorAssembler}


object FeatureExtraction {

  def getFeatures(pipelineModel:PipelineModel):Array[String] = {
    val vectorAssembler = pipelineModel.stages.find(_.isInstanceOf[VectorAssembler]).getOrElse(throw new IllegalArgumentException("Invalid model")).asInstanceOf[VectorAssembler]
    val featureNames = vectorAssembler.getInputCols

    featureNames.flatMap(featureName => {
      val oneHotEncoder = pipelineModel.stages.filter(_.isInstanceOf[OneHotEncoder]).map(_.asInstanceOf[OneHotEncoder]).find(_.getOutputCol == featureName)
      val oneHotEncoderInputCol = oneHotEncoder.map(_.getInputCol).getOrElse(featureName)

      val stringIndexer = pipelineModel.stages.filter(_.isInstanceOf[StringIndexerModel]).map(_.asInstanceOf[StringIndexerModel]).find(_.getOutputCol == oneHotEncoderInputCol)
      val stringIndexerInput = stringIndexer.map(_.getInputCol).getOrElse(featureName)

      oneHotEncoder.map(encoder => {
        val labelValues = stringIndexer.getOrElse(throw new IllegalArgumentException("Invalid model")).labels
        labelValues.map(label => s"$stringIndexerInput-$label")
      }).getOrElse(Array(stringIndexerInput))
    })
  }
}
