package org.apache.mleap.runtime

import org.apache.mleap.core.linalg.Vector

/**
  * Created by hwilkins on 11/2/15.
  */
case class Row(data: Array[Any]) {
  private[mleap] def apply(index: Int): Any = get(index)
  private[mleap] def get(index: Int): Any = data(index)

  def toArray: Array[Any] = data.toArray

  def getDouble(index: Int): Double = data(index).asInstanceOf[Double]
  def getString(index: Int): String = data(index).asInstanceOf[String]
  def getVector(index: Int): Vector = data(index).asInstanceOf[Vector]

  def withValue(f: (Row) => Any): Row = withValue(f(this))
  def withValue(value: Any): Row = {
    Row(data :+ value)
  }

  def selectIndices(indices: Int *): Row = {
    val values = indices.toArray.map(data)
    Row(values)
  }

  def dropIndex(index: Int): Row = {
    val values = toArray.zipWithIndex.filter(_._2 != index).map(_._1)
    Row(values)
  }

  override def toString: String = s"Row(${mkString(",")})"

  def mkString: String = data.mkString
  def mkString(sep: String): String = data.mkString(sep)
  def mkString(start: String, sep: String, end: String): String = data.mkString(start, sep, end)
}