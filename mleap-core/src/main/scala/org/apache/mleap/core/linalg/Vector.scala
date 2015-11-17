package org.apache.mleap.core.linalg

/**
 * Created by hwilkins on 11/5/15.
 */
object Vector {
  import scala.language.implicitConversions

  def dense(values: Array[Double]): DenseVector = DenseVector(values)
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): SparseVector = SparseVector(size, indices, values)

  implicit def vectorToBreeze(vector: Vector): breeze.linalg.Vector[Double] = vector.toBreeze
}

trait Vector extends Serializable {
  def apply(index: Int): Double = toBreeze(index)
  def toBreeze: breeze.linalg.Vector[Double]
  def foreachActive(f: (Int, Double) => Unit)

  def size: Int

  def toArray: Array[Double]
  def toSparse: SparseVector
  def toDense: DenseVector = DenseVector(toArray)

  def numActives: Int
  def numNonzeros: Int

  def compressed: Vector = {
    val nnz = numNonzeros
    // A dense vector needs 8 * size + 8 bytes, while a sparse vector needs 12 * nnz + 20 bytes.
    if (1.5 * (nnz + 1.0) < size) {
      toSparse
    } else {
      toDense
    }
  }
}

case class SparseVector(size: Int,
                        indices: Array[Int],
                        values: Array[Double]) extends Vector {
  override def toBreeze: breeze.linalg.SparseVector[Double] = new breeze.linalg.SparseVector[Double](indices, values, size)

  override def toArray: Array[Double] = {
    val data = new Array[Double](size)
    var i = 0
    val nnz = indices.length
    while (i < nnz) {
      data(indices(i)) = values(i)
      i += 1
    }
    data
  }

  override def toSparse: SparseVector = {
    val nnz = numNonzeros
    if (nnz == numActives) {
      this
    } else {
      val ii = new Array[Int](nnz)
      val vv = new Array[Double](nnz)
      var k = 0
      foreachActive { (i, v) =>
        if (v != 0.0) {
          ii(k) = i
          vv(k) = v
          k += 1
        }
      }
      new SparseVector(size, ii, vv)
    }
  }

  override def numActives: Int = values.length

  override def numNonzeros: Int = {
    var nnz = 0
    values.foreach { v =>
      if (v != 0.0) {
        nnz += 1
      }
    }
    nnz
  }

  override def foreachActive(f: (Int, Double) => Unit): Unit = {
    var i = 0
    val localValuesSize = values.length
    val localIndices = indices
    val localValues = values

    while (i < localValuesSize) {
      f(localIndices(i), localValues(i))
      i += 1
    }
  }
}
case class DenseVector(values: Array[Double]) extends Vector {
  override def toBreeze: breeze.linalg.DenseVector[Double] = new breeze.linalg.DenseVector[Double](values)

  override def size: Int = values.length

  override def toArray: Array[Double] = values

  override def toSparse: SparseVector = {
    val nnz = numNonzeros
    val ii = new Array[Int](nnz)
    val vv = new Array[Double](nnz)
    var k = 0
    foreachActive { (i, v) =>
      if (v != 0) {
        ii(k) = i
        vv(k) = v
        k += 1
      }
    }
    SparseVector(size, ii, vv)
  }

  override def numActives: Int = size

  override def numNonzeros: Int = {
    // same as values.count(_ != 0.0) but faster
    var nnz = 0
    values.foreach { v =>
      if (v != 0.0) {
        nnz += 1
      }
    }
    nnz
  }

  override def foreachActive(f: (Int, Double) => Unit): Unit = {
    var i = 0
    val localValuesSize = values.length
    val localValues = values

    while (i < localValuesSize) {
      f(i, localValues(i))
      i += 1
    }
  }
}
