package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.VectorAssembler
import org.apache.mleap.runtime.types.{StructType, VectorType, StructField}
import org.apache.mleap.runtime.{LeapFrame, Row, Transformer}

/**
 * Created by hwilkins on 10/23/15.
 */
case class VectorAssemblerModel(inputSchema: StructType,
                                outputCol: String) extends Transformer {
  val assembler: VectorAssembler = VectorAssembler.default

  override def transform(features: LeapFrame): LeapFrame = {
    val inputIndices = inputSchema.fields.map(_.name).map(features.schema.indexOf)
    val assemble = {
      (row: Row) =>
        assembler(inputIndices.map(row.get))
    }

    features.withFeature(StructField(outputCol, VectorType), assemble)
  }
}
