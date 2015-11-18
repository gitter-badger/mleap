package org.apache.spark.ml.mleap.converter

import org.apache.mleap.runtime.{Transformer => MleapTransformer}
import org.apache.spark.sql.DataFrame
import MleapSparkSupport._

/**
  * Created by hwilkins on 11/18/15.
  */
case class MleapTransformerWrapper(transformer: MleapTransformer) {
  def sparkTransform(dataset: DataFrame): DataFrame = {
    transformer.transform(dataset.toMleap(transformer.schema.input)).get.toSpark(dataset.sqlContext)
  }
}
