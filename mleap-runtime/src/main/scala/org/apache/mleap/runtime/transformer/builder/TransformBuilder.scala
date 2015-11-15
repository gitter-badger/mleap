package org.apache.mleap.runtime.transformer.builder

import org.apache.mleap.runtime.types.{StructType, DataType}
import org.apache.mleap.runtime.Row

import scala.util.{Failure, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
sealed trait Validation
final case class Invalid(dataType: DataType) extends Validation
object Valid extends Validation
object Dropped extends Validation

trait TransformBuilder[T <: TransformBuilder[T]] {
  def validateField(name: String, dataType: DataType): Validation
  def hasField(name: String): Boolean

  def withInput(name: String, dataType: DataType): Try[(T, Int)] = {
    validateField(name, dataType) match {
      case Valid => withInputInternal(name, dataType)
      case Dropped =>
        Failure(new Error(s"Field $name was dropped"))
      case Invalid(actual) =>
        Failure(new Error(s"Field $name has wrong type, expected $dataType found $actual"))
    }
  }
  def endWithInput(name: String, dataType: DataType): Try[T] =
    withInputInternal(name, dataType).map(_._1)
  protected def withInputInternal(name: String, dataType: DataType): Try[(T, Int)]

  def withOutput(name: String, dataType: DataType)
                (o: (Row) => Any): Try[(T, Int)] = {
    if(hasField(name)) {
      Failure(new Error(s"Field already exists $name"))
    } else {
      withOutputInternal(name, dataType)(o)
    }
  }
  def endWithOutput(name: String, dataType: DataType)
                   (o: (Row) => Any): Try[T] =
    withOutput(name, dataType)(o).map(_._1)
  protected def withOutputInternal(name: String, dataType: DataType)
                                  (o: (Row) => Any): Try[(T, Int)]

  def withSelect(schema: StructType): Try[T] = {
    schema.fields.foldLeft(Try(this.asInstanceOf[T])) {
      (builder, field) =>
        validateField(field.name, field.dataType) match {
          case Valid => withSelectInternal(schema)
          case Dropped =>
            Failure(new Error(s"Field ${field.name} was dropped"))
          case Invalid(actual) =>
            Failure(new Error(s"Field ${field.name} has wrong type, expected ${field.dataType} found $actual"))
        }
    }
  }
  def withSelectInternal(schema: StructType): Try[T]
}
