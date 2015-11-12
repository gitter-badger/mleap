package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.StandardScaler
import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.runtime.types.{StructType, VectorType, StructField}
import org.apache.mleap.runtime.{LeapFrame, Row, Transformer}

/**
 * Created by hwilkins on 10/23/15.
 */
case class StandardScalerModel(inputCol: String,
                               outputCol: String,
                               scaler: StandardScaler) extends Transformer {
  override def inputSchema: StructType = StructType.withFields(StructField(inputCol, VectorType))

  override def transform(features: LeapFrame): LeapFrame = {
    val inputIndex = features.schema.indexOf(inputCol)
    val scale = {
      (row: Row) =>
        scaler(row.getAs[Vector](inputIndex))
    }

    features.withFeature(StructField(outputCol, VectorType), scale)
  }
}
