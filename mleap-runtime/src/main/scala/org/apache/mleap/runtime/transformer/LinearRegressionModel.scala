package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.core.regression.LinearRegression
import org.apache.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.mleap.runtime._
import org.apache.mleap.runtime.types.{DoubleType, VectorType}

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class LinearRegressionModel(featuresCol: String,
                                 predictionCol: String,
                                 model: LinearRegression) extends Transformer {
  override def build[T <: TransformBuilder[T]](builder: T): Try[T] = {
    builder.withInput(featuresCol, VectorType).flatMap {
      case(b, featuresIndex) =>
        b.endWithOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
    }
  }
}
