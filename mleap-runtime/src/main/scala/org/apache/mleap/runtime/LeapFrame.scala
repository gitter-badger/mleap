package org.apache.mleap.runtime

import org.apache.mleap.runtime.types.{StructField, StructType}

import scala.util.{Failure, Success, Try}


/**
  * Created by hwilkins on 11/2/15.
  */
trait LeapFrame[T <: LeapFrame[T]] {
  type D <: Dataset[D]

  def schema: StructType
  def dataset: D

  def select(fieldNames: String *): Try[T] = {
    schema.tryIndicesOf(fieldNames: _*).flatMap {
      indices =>
        val schema2 = schema.selectIndices(indices: _*)
        val dataset2 = dataset.selectIndices(indices: _*)

        selectInternal(schema2, dataset2, fieldNames: _*)
    }
  }

  def withField(field: StructField, f: (Row) => Any): Try[T] = {
    schema.indexOf(field.name) match {
      case None =>
        val schema2 = schema.withField(field)
        val dataset2 = dataset.withValue(f)

        withFieldInternal(schema2, dataset2, field)
      case Some(index) =>
        Failure(new Error(s"Field ${field.name} already exists"))
    }
  }

  def dropField(name: String): Try[T] = {
    schema.tryIndexOf(name).flatMap {
      index =>
        val schema2 = schema.dropIndex(index)
        val dataset2 = dataset.dropIndex(index)

        dropFieldInternal(schema2, dataset2, name)
    }
  }

  protected def selectInternal(schema2: StructType, dataset2: D, fieldNames: String *): Try[T]
  protected def withFieldInternal(schema2: StructType, dataset2: D, field: StructField): Try[T]
  protected def dropFieldInternal(schema2: StructType, dataset2: D, fieldName: String): Try[T]

  def toLocal: LocalLeapFrame
}

case class LocalLeapFrame(schema: StructType, dataset: ArrayDataset) extends LeapFrame[LocalLeapFrame] {
  override def toLocal: LocalLeapFrame = this

  override type D = ArrayDataset

  override protected def withFieldInternal(schema2: StructType,
                                           dataset2: D,
                                           field: StructField): Try[LocalLeapFrame] = {
    Success(LocalLeapFrame(schema2, dataset2))
  }
  override protected def selectInternal(schema2: StructType,
                                        dataset2: D,
                                        fieldNames: String *): Try[LocalLeapFrame] = {
    Success(LocalLeapFrame(schema2, dataset2))
  }
  override protected def dropFieldInternal(schema2: StructType,
                                           dataset2: D,
                                           fieldName: String): Try[LocalLeapFrame] = {
    Success(LocalLeapFrame(schema2, dataset2))
  }
}
