package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.StandardScaler
import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.mleap.runtime.types.{DoubleType, StructType, VectorType, StructField}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class StandardScalerModel(inputCol: String,
                               outputCol: String,
                               scaler: StandardScaler) extends Transformer {
  override def transform[T <: TransformBuilder[T]](builder: T): Try[T] = {
    builder.withInput(inputCol, VectorType).flatMap {
      case (b, inputIndex) =>
        b.endWithOutput(outputCol, VectorType)(row => scaler(row.getAs[Vector](inputIndex)))
    }
  }
}
