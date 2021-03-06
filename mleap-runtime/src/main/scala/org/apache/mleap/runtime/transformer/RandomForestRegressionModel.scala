package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.regression.RandomForestRegression
import org.apache.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.mleap.runtime.types.{VectorType, DoubleType}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 11/8/15.
  */
case class RandomForestRegressionModel(featuresCol: String,
                                       predictionCol: String,
                                       model: RandomForestRegression) extends Transformer {
  override def build[T <: TransformBuilder[T]](builder: T): Try[T] = {
    builder.withInput(featuresCol, VectorType).flatMap {
      case(b, featuresIndex) =>
        b.endWithOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
    }
  }
}
