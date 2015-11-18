package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import MleapSparkSupport._

/**
  * Created by hwilkins on 11/18/15.
  */
case class DecisionTreeRegressionModelToMleap(tree: DecisionTreeRegressionModel) {
  def toMleap: core.regression.DecisionTreeRegression = {
    core.regression.DecisionTreeRegression(tree.rootNode.toMleap)
  }
}
