package org.apache.mleap.runtime.transformer

import org.apache.mleap.runtime.Transformer
import org.apache.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.mleap.runtime.types.StructType

import scala.util.Try

/**
  * Created by hwilkins on 11/15/15.
  */
case class SelectorModel(fieldNames: Seq[String]) extends Transformer {
  override def build[T <: TransformBuilder[T]](builder: T): Try[T] = {
    builder.withSelect(fieldNames)
  }
}
