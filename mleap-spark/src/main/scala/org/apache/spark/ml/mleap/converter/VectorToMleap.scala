package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core.linalg.{Vector => MleapVector}
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector}

/**
  * Created by hwilkins on 11/18/15.
  */
case class VectorToMleap(vector: Vector) {
  def toMleap: MleapVector = {
    vector match {
      case DenseVector(values) => MleapVector.dense(values)
      case SparseVector(size, indices, values) => MleapVector.sparse(size, indices, values)
    }
  }
}
