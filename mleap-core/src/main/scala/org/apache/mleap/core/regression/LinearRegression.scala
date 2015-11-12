package org.apache.mleap.core.regression

import org.apache.mleap.core.linalg.Vector

/**
 * Created by hwilkins on 11/5/15.
 */
case class LinearRegression(weights: Vector,
                            intercept: Double) extends Serializable {
  def apply(features: Vector): Double = {
    features.toBreeze.dot(weights.toBreeze) + intercept
  }
}
