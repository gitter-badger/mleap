package org.apache.mleap.runtime.transformer

import org.apache.mleap.runtime.types.StructType
import org.apache.mleap.runtime.{LeapFrame, Transformer}

/**
  * Created by hwilkins on 11/14/15.
  */
case class OutputSelector(inputSchema: StructType) extends Transformer {
  override def transform(dataset: LeapFrame): LeapFrame = {
    dataset.select(inputSchema.fields.map(_.name): _*)
  }
}
