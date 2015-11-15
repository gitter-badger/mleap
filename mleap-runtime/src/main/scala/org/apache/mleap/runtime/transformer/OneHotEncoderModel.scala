package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.OneHotEncoder
import org.apache.mleap.runtime.types.{DoubleType, VectorType, StructField}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class OneHotEncoderModel(inputCol: String,
                              outputCol: String,
                              encoder: OneHotEncoder) extends Transformer {
  override def calculateSchema(calc: SchemaCalculator): Try[SchemaCalculator] = {
    calc.withInputField(inputCol, DoubleType)
      .flatMap(_.withOutputField(outputCol, VectorType))
  }

  override def transform(features: LeapFrame): LeapFrame = {
    val inputIndex = features.schema.indexOf(inputCol)
    val encode = {
      (row: Row) =>
        encoder(row.getAs[Double](inputIndex))
    }

    features.withFeature(StructField(outputCol, VectorType), encode)
  }
}
