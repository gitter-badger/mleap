package org.apache.mleap.runtime

/**
 * Created by hwilkins on 11/2/15.
 */
case class Row(data: Any *) extends Serializable {
  def apply(index: Int): Any = get(index)
  def get(index: Int): Any = data(index)

  def getAs[T](index: Int): T = data(index).asInstanceOf

  def withValue(value: Any): Row = {
    Row(data :+ value: _*)
  }

  def select(indices: Int *): Row = {
    val values = indices.toArray.map(this.get)
    Row(values: _*)
  }
}
