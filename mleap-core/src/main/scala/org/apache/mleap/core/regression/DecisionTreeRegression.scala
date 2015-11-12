package org.apache.mleap.core.regression

import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.core.tree.{DecisionTree, Node}

/**
 * Created by hwilkins on 11/8/15.
 */
case class DecisionTreeRegression(rootNode: Node) extends DecisionTree {
  def predict(features: Vector): Double = {
    rootNode.predictImpl(features).prediction
  }
}
