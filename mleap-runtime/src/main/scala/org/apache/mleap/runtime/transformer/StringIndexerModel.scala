package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.StringIndexer
import org.apache.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.mleap.runtime.types.{StructType, StringType, StructField, DoubleType}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexerModel(inputCol: String,
                              outputCol: String,
                              indexer: StringIndexer) extends Transformer {
  override def transform[T <: TransformBuilder[T]](builder: T): Try[T] = {
    builder.withInput(inputCol, StringType).flatMap {
      case (b, inputIndex) =>
        b.endWithOutput(outputCol, DoubleType)(row => indexer(row.getAs[String](inputIndex)))
    }
  }
}
