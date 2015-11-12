package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.OneHotEncoder
import org.apache.mleap.runtime.types.{StructType, DoubleType, VectorType, StructField}
import org.apache.mleap.runtime.{LeapFrame, Row, Transformer}

/**
  * Created by hwilkins on 10/23/15.
  */
case class OneHotEncoderModel(inputCol: String,
                              outputCol: String,
                              encoder: OneHotEncoder) extends Transformer {
  override def inputSchema: StructType = StructType.withFields(StructField(inputCol, DoubleType))

  override def transform(features: LeapFrame): LeapFrame = {
    val inputIndex = features.schema.indexOf(inputCol)
    val encode = {
      (row: Row) =>
        encoder(row.getAs[Double](inputIndex))
    }

    features.withFeature(StructField(outputCol, VectorType), encode)
  }
}
