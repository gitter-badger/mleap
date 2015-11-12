package org.apache.mleap.core.regression

import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.core.tree.TreeEnsemble

/**
 * Created by hwilkins on 11/8/15.
 */
case class RandomForestRegression(trees: Seq[DecisionTreeRegression],
                                  treeWeights: Seq[Double]) extends TreeEnsemble {
  val numTrees = trees.length

  def predict(features: Vector): Double = {
    trees.map(_.predict(features)).sum / numTrees
  }
}
