package org.apache.mleap.runtime

import org.apache.mleap.runtime.types.{StructField, StructType}


/**
  * Created by hwilkins on 11/2/15.
  */
trait LeapFrame[T <: LeapFrame[T]] {
  def schema: StructType
  def dataset: Dataset

  def select(fields: String *): T
  def withFeature(field: StructField, f: (Row) => Any): T
  def dropFeature(field: String): T
  def toLocal: LocalLeapFrame
}

case class LocalLeapFrame(schema: StructType, dataset: ArrayDataset) extends LeapFrame[LocalLeapFrame] {
  override def toLocal: LocalLeapFrame = this

  override def select(fields: String *): LocalLeapFrame = {
    val indices = fields.map(schema.indexOf)
    val schema2 = schema.select(indices: _*)
    val dataset2 = dataset.map(_.select(indices: _*))

    LocalLeapFrame(schema2, dataset2)
  }

  def withFeature(field: StructField, f: (Row) => Any): LocalLeapFrame = {
    val schema2 = StructType(schema.fields :+ field)
    val dataset2 = dataset.map {
      row => row.withValue(f(row))
    }

    LocalLeapFrame(schema2, dataset2)
  }

  override def dropFeature(field: String): LocalLeapFrame = {
    val index = schema.indexOf(field)
    val size = schema.fields.length - 1
    val fields = schema.fields.filter(_.name != field)
    val schema2 = StructType(fields)
    val dataset2 = dataset.map {
      row =>
        val values = new Array[Any](size)
        val oldValues = row.toArray

        if(index == 0) {
          oldValues.slice(1, oldValues.length).copyToArray(values)
        } else if(index == values.length) {
          oldValues.slice(0, oldValues.length - 1).copyToArray(values)
        } else {
          oldValues.slice(0, index).copyToArray(values)
          oldValues.slice(index + 1, oldValues.length).copyToArray(values, index)
        }

        Row(values)
    }

    LocalLeapFrame(schema2, dataset2)
  }
}
