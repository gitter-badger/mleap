package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.StringIndexer
import org.apache.mleap.runtime.types.{StructType, StringType, StructField, DoubleType}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexerModel(inputCol: String,
                              outputCol: String,
                              indexer: StringIndexer) extends Transformer {
  override def calculateSchema(calc: SchemaCalculator): Try[SchemaCalculator] = {
    calc.withInputField(inputCol, StringType)
      .flatMap(_.withOutputField(outputCol, DoubleType))
  }

  override def transform(features: LeapFrame): LeapFrame = {
    val inputIndex = features.schema.indexOf(inputCol)
    val index = {
      (row: Row) =>
        indexer(row(inputIndex).toString)
    }

    features.withFeature(StructField(outputCol, DoubleType), index)
  }
}
