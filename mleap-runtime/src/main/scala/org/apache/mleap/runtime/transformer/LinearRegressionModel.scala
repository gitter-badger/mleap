package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.core.regression.LinearRegression
import org.apache.mleap.runtime.types.{VectorType, StructField, DoubleType}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class LinearRegressionModel(featuresCol: String,
                                 predictionCol: String,
                                 model: LinearRegression) extends Transformer {
  override def calculateSchema(calc: SchemaCalculator): Try[SchemaCalculator] = {
    calc.withInputField(featuresCol, VectorType)
      .flatMap(_.withOutputField(predictionCol, DoubleType))
  }

  override def transform(features: LeapFrame): LeapFrame = {
    val featuresIndex = features.schema.indexOf(featuresCol)
    val predict = {
      (row: Row) =>
        model(row.getAs[Vector](featuresIndex))
    }

    features.withFeature(StructField(predictionCol, DoubleType), predict)
  }
}
