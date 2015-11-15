package org.apache.mleap.runtime

import org.apache.mleap.core.linalg.Vector

/**
 * Created by hwilkins on 11/2/15.
 */

case class Row(data: Any *) extends Serializable {
  private[mleap] def apply(index: Int): Any = get(index)
  private[mleap] def get(index: Int): Any = data(index)

  def getDouble(index: Int): Double = data(index).asInstanceOf
  def getString(index: Int): String = data(index).asInstanceOf
  def getVector(index: Int): Vector = data(index).asInstanceOf

  def withValue(value: Any): Row = {
    Row(data :+ value: _*)
  }

  def select(indices: Int *): Row = {
    val values = indices.toArray.map(data)
    Row(values: _*)
  }
}
