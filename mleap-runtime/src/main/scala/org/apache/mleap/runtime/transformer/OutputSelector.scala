package org.apache.mleap.runtime.transformer

import org.apache.mleap.runtime.types.StructType
import org.apache.mleap.runtime.{SchemaCalculator, LeapFrame, Transformer}

import scala.util.Try

/**
  * Created by hwilkins on 11/14/15.
  */
case class OutputSelector(inputSchema: StructType) extends Transformer {
  override def calculateSchema(calc: SchemaCalculator): Try[SchemaCalculator] = {
    val cleared = inputSchema.fields.foldLeft(Try(calc))((c, field) => c.flatMap(_.withInputField(field)))
      .flatMap(_.withoutAllFields())
    inputSchema.fields.foldLeft(cleared)((c, field) => c.flatMap(_.withOutputField(field)))
  }

  override def transform(dataset: LeapFrame): LeapFrame = {
    dataset.select(inputSchema.fields.map(_.name): _*)
  }
}
