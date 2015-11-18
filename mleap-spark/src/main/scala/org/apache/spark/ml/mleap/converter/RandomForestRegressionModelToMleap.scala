package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel}
import MleapSparkSupport._

/**
  * Created by hwilkins on 11/18/15.
  */
case class RandomForestRegressionModelToMleap(forest: RandomForestRegressionModel) {
  def toMleap: core.regression.RandomForestRegression = {
    core.regression.RandomForestRegression(forest.trees.asInstanceOf[Array[DecisionTreeRegressionModel]].map(_.toMleap), forest.treeWeights)
  }
}
