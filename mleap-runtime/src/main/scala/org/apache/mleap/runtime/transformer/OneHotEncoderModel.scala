package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.OneHotEncoder
import org.apache.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.mleap.runtime._
import org.apache.mleap.runtime.types.{VectorType, DoubleType}

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class OneHotEncoderModel(inputCol: String,
                              outputCol: String,
                              encoder: OneHotEncoder) extends Transformer {
  override def transform[T <: TransformBuilder[T]](builder: T): Try[T] = {
    builder.withInput(inputCol, DoubleType).flatMap {
      case (b, inputIndex) =>
        b.endWithOutput(outputCol, VectorType)(row => encoder(row.getDouble(inputIndex)))
    }
  }
}
