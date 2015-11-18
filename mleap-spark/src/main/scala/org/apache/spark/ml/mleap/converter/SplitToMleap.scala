package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core
import org.apache.spark.ml.tree.{ContinuousSplit, CategoricalSplit, Split}

/**
  * Created by hwilkins on 11/18/15.
  */
case class SplitToMleap(split: Split) {
  def toMleap: core.tree.Split = {
    split match {
      case split: CategoricalSplit =>
        val (isLeft, categories) = if(split.leftCategories.length >= split.rightCategories.length) {
          (true, split.leftCategories)
        } else {
          (false, split.rightCategories)
        }
        core.tree.CategoricalSplit(split.featureIndex, categories, isLeft)
      case split: ContinuousSplit =>
        core.tree.ContinuousSplit(split.featureIndex, split.threshold)
    }
  }
}
