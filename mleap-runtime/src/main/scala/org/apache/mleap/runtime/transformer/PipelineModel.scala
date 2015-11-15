package org.apache.mleap.runtime.transformer

import org.apache.mleap.runtime.types.StructType
import org.apache.mleap.runtime.{SchemaCalculator, LeapFrame, Transformer}

import scala.util.Try

/**
 * Created by hwilkins on 11/8/15.
 */
case class PipelineModel(stages: Seq[Transformer]) extends Transformer {
  override def calculateSchema(calc: SchemaCalculator): Try[SchemaCalculator] = {
    stages.foldLeft(Try(calc))((c, transformer) => c.flatMap(transformer.calculateSchema))
  }

  override def transform(dataset: LeapFrame): LeapFrame = {
    stages.foldLeft(dataset) {
      (dataset2, stage) => stage.transform(dataset2)
    }
  }
}
