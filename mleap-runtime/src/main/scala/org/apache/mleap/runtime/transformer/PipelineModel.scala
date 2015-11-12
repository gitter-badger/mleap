package org.apache.mleap.runtime.transformer

import org.apache.mleap.runtime.types.StructType
import org.apache.mleap.runtime.{LeapFrame, Transformer}

/**
 * Created by hwilkins on 11/8/15.
 */
case class PipelineModel(stages: Array[Transformer]) extends Transformer {
  override def inputSchema: StructType = StructType.empty

  override def transform(dataset: LeapFrame): LeapFrame = {
    stages.foldLeft(dataset) {
      (dataset2, stage) => stage.transform(dataset2)
    }
  }
}
