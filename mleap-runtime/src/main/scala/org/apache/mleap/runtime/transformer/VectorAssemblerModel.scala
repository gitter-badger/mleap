package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.VectorAssembler
import org.apache.mleap.runtime.types.{StructType, VectorType, StructField}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class VectorAssemblerModel(inputSchema: StructType,
                                outputCol: String) extends Transformer {
  override def calculateSchema(calc: SchemaCalculator): Try[SchemaCalculator] = {
    val calc2 = inputSchema.fields.foldLeft(Try(calc))((c, field) => c.flatMap(_.withInputField(field)))
    calc2.flatMap(_.withOutputField(outputCol, VectorType))
  }

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
