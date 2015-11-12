package org.apache.mleap.runtime

import org.apache.mleap.runtime.types.StructType

/**
  * Created by hwilkins on 10/22/15.
  */
object Transformer {
  val linearRegressionModelName = "LinearRegressionModel"
  val oneHotEncoderModelName = "OneHotEncoderModel"
  val pipelineModelName = "PipelineModel"
  val randomForestRegressionModelName = "RandomForestRegressionModel"
  val standardScalerModelName = "StandardScalerModel"
  val stringIndexerModelName = "StringIndexerModel"
  val vectorAssemblerModelName = "VectorAssemblerModel"
}

trait Transformer extends Serializable {
  def inputSchema: StructType

  def transform(dataset: LeapFrame): LeapFrame
}
