package org.apache.mleap.runtime

import org.apache.mleap.runtime.transformer.builder.{TransformerSchemaBuilder, LeapFrameBuilder, TransformBuilder}
import org.apache.mleap.runtime.types.StructType

import scala.util.Try


/**
  * Created by hwilkins on 10/22/15.
  */
case class TransformerSchema(input: StructType, output: StructType)

object Transformer {
  val linearRegressionModelName = "LinearRegressionModel"
  val oneHotEncoderModelName = "OneHotEncoderModel"
  val outputSelectorName = "OutputSelector"
  val pipelineModelName = "PipelineModel"
  val randomForestRegressionModelName = "RandomForestRegressionModel"
  val standardScalerModelName = "StandardScalerModel"
  val stringIndexerModelName = "StringIndexerModel"
  val vectorAssemblerModelName = "VectorAssemblerModel"
}

trait Transformer extends Serializable {
  def schema(): Try[TransformerSchema] = build(TransformerSchemaBuilder()).map(_.build())
  def transform[T <: LeapFrame[T]](frame: T): Try[T] =
    build(LeapFrameBuilder(frame)).map(_.frame)
  def build[T <: TransformBuilder[T]](builder: T): Try[T]
}
