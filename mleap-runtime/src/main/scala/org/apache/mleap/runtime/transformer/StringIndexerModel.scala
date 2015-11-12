package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.StringIndexer
import org.apache.mleap.runtime.types.{StructType, StringType, StructField, DoubleType}
import org.apache.mleap.runtime.{Row, LeapFrame, Transformer}

/**
 * Created by hwilkins on 10/22/15.
 */
case class StringIndexerModel(inputCol: String,
                              outputCol: String,
                              indexer: StringIndexer) extends Transformer {
  override def inputSchema: StructType = StructType.withFields(StructField(inputCol, StringType))

  override def transform(features: LeapFrame): LeapFrame = {
    val inputIndex = features.schema.indexOf(inputCol)
    val index = {
      (row: Row) =>
        indexer(row(inputIndex).toString)
    }

    features.withFeature(StructField(outputCol, DoubleType), index)
  }
}
