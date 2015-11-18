package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core
import org.apache.mleap.runtime.{transformer => tform, Transformer => MleapTransformer}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.mleap.{OneHotEncoderModel, VectorAssemblerModel}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, LinearRegressionModel}
import MleapSparkSupport._

/**
  * Created by hwilkins on 11/18/15.
  */
case class TransformerToMleap(transformer: Transformer) {
  def toMleap: MleapTransformer = {
    transformer match {
      case transformer: LinearRegressionModel => tform.LinearRegressionModel(transformer.getFeaturesCol,
        transformer.getPredictionCol,
        core.regression.LinearRegression(transformer.weights.toMleap,
          transformer.intercept))
      case transformer: StandardScalerModel =>
        tform.StandardScalerModel(transformer.getInputCol,
          transformer.getOutputCol,
          core.feature.StandardScaler(Option(transformer.std.toMleap),
            Option(transformer.mean.toMleap)))
      case transformer: org.apache.spark.ml.mleap.StringIndexerModel =>
        tform.StringIndexerModel(transformer.getInputCol,
          transformer.getOutputCol,
          core.feature.StringIndexer(transformer.getLabels))
      case transformer: VectorAssemblerModel =>
        tform.VectorAssemblerModel(transformer.getInputSchema.toMleap,
          transformer.getOutputCol)
      case transformer: OneHotEncoderModel => tform.OneHotEncoderModel(transformer.getInputCol,
        transformer.getOutputCol,
        core.feature.OneHotEncoder(transformer.getSize))
      case transformer: PipelineModel => tform.PipelineModel(transformer.stages.map(_.toMleap))
      case transformer: RandomForestRegressionModel => tform.RandomForestRegressionModel(transformer.getFeaturesCol,
        transformer.getPredictionCol,
        RandomForestRegressionModelToMleap(transformer).toMleap)
    }
  }
}
