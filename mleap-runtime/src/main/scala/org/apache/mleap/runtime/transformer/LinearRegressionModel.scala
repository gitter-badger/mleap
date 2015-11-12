package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.core.regression.LinearRegression
import org.apache.mleap.runtime.types.{StructType, VectorType, StructField, DoubleType}
import org.apache.mleap.runtime.{LeapFrame, Row, Transformer}

/**
  * Created by hwilkins on 10/22/15.
  */
case class LinearRegressionModel(featuresCol: String,
                                 predictionCol: String,
                                 model: LinearRegression) extends Transformer {
  override def inputSchema: StructType = StructType.withFields(StructField(featuresCol, VectorType))

  override def transform(features: LeapFrame): LeapFrame = {
    val featuresIndex = features.schema.indexOf(featuresCol)
    val predict = {
      (row: Row) =>
        model(row.getAs[Vector](featuresIndex))
    }

    features.withFeature(StructField(predictionCol, DoubleType), predict)
  }
}
