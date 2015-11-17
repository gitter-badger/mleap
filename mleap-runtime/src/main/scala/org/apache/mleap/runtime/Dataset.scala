package org.apache.mleap.runtime

/**
  * Created by hwilkins on 11/2/15.
  */
trait Dataset[T <: Dataset[T]] extends Serializable {
  def map(f: (Row) => Row): T

  def withValue(f: (Row) => Any): T = map(_.withValue(f))
  def selectIndices(indices: Int *): T = map(_.selectIndices(indices: _*))
  def dropIndex(index: Int): T = map(_.dropIndex(index))

  def toArray: Array[Row]
}

case class ArrayDataset(data: Array[Row]) extends Dataset[ArrayDataset] {
  override def map(f: (Row) => Row): ArrayDataset = {
    val data2 = data.map(f)
    copy(data = data2)
  }

  override def toArray: Array[Row] = data
}