package org.apache.mleap.runtime.transformer

import org.apache.mleap.runtime.Transformer
import org.apache.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
 * Created by hwilkins on 11/8/15.
 */
case class PipelineModel(stages: Seq[Transformer]) extends Transformer {

  override def build[T <: TransformBuilder[T]](builder: T): Try[T] = {
    stages.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.build[T]))
  }
}
