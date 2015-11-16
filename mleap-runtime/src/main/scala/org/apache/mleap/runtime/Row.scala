package org.apache.mleap.runtime

import org.apache.mleap.core.linalg.Vector

/**
 * Created by hwilkins on 11/2/15.
 */
object Row {
  def apply(data: Any *): Row = Row(data.toArray)
}

case class Row(data: Array[Any]) {
  private[mleap] def apply(index: Int): Any = get(index)
  private[mleap] def get(index: Int): Any = data(index)

  def toArray: Array[Any] = data.toArray

  def getDouble(index: Int): Double = data(index).asInstanceOf
  def getString(index: Int): String = data(index).asInstanceOf
  def getVector(index: Int): Vector = data(index).asInstanceOf

  def withValue(value: Any): Row = {
    Row(data :+ value)
  }

  def select(indices: Int *): Row = {
    val values = indices.toArray.map(data)
    Row(values)
  }
}
