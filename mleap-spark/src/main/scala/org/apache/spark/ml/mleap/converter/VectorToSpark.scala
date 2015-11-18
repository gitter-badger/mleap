package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core.linalg.{SparseVector => MleapSparseVector, DenseVector => MleapDenseVector, Vector => MleapVector}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
  * Created by hwilkins on 11/18/15.
  */
case class VectorToSpark(vector: MleapVector) {
  def toSpark: Vector = vector match {
    case MleapDenseVector(values) => Vectors.dense(values)
    case MleapSparseVector(size, indices, values) => Vectors.sparse(size, indices, values)
  }
}