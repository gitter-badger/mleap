package org.apache.mleap.runtime

import org.apache.mleap.runtime.types.{StructField, StructType}


/**
  * Created by hwilkins on 11/2/15.
  */
trait LeapFrame {
  def schema: StructType
  def dataset: Dataset

  def select(fields: String *): LeapFrame
  def withFeature(field: StructField, f: (Row) => Any): LeapFrame
  def toLocal: LocalLeapFrame
}
object LeapFrame {
  val empty: LeapFrame = LocalLeapFrame.empty
}

object LocalLeapFrame {
  val empty: LocalLeapFrame = LocalLeapFrame(StructType.empty, ArrayDataset.empty)
}

case class LocalLeapFrame(schema: StructType, dataset: ArrayDataset) extends LeapFrame {
  override def toLocal: LocalLeapFrame = this

  override def select(fields: String*): LocalLeapFrame = {
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
}
