package org.apache.mleap.runtime

import org.apache.mleap.runtime.SchemaCalculator.FieldStatus
import org.apache.mleap.runtime.SchemaCalculator.FieldStatus.{Free, Dropped, Available}
import org.apache.mleap.runtime.types.{DataType, StructType, StructField}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
case class SchemaCalculator(input: Map[String, StructField] = Map(),
                            available: Map[String, StructField] = Map(),
                            drop: Set[String] = Set(),
                            dropped: Boolean = false) {
  def toSchema: TransformerSchema = {
    val outputFields = available.filterKeys(!input.contains(_))
    val inputSchema = StructType(input.values.toSeq)
    val outputSchema = StructType(outputFields.values.toSeq)

    TransformerSchema(inputSchema, outputSchema)
  }

  def fieldStatus(name: String): FieldStatus = {
    if(available.contains(name)) {
      Available(available(name))
    } else if(drop.contains(name) || dropped) {
      Dropped
    } else {
      Free
    }
  }

  def withInputField(name: String, dataType: DataType): Try[SchemaCalculator] =
    withInputField(StructField(name, dataType))
  def withInputField(field: StructField): Try[SchemaCalculator] = {
    fieldStatus(field.name) match {
      case Available(availableField) =>
        if(field.dataType == availableField.dataType) {
          Success(this)
        } else {
          Failure(new Error(s"Field ${field.name} expected to be ${field.dataType}, found ${availableField.dataType}"))
        }
      case Free =>
        Success(copy(input = input + (field.name -> field),
          available = available + (field.name -> field)))
      case Dropped => Failure(new Error(s"Field $field dropped"))
    }
  }

  def withOutputField(name: String, dataType: DataType): Try[SchemaCalculator] =
    withOutputField(StructField(name, dataType))
  def withOutputField(field: StructField): Try[SchemaCalculator] = {
    fieldStatus(field.name) match {
      case Available(availableField) =>
        Failure(new Error(s"Field ${field.name} is already available as $availableField"))
      case Free =>
        Success(copy(available = available + (field.name -> field)))
      case Dropped =>
        Success(copy(available = available + (field.name -> field),
          drop = drop - field.name))
    }
  }

  def withoutField(name: String): Try[SchemaCalculator] = {
    fieldStatus(name) match {
      case Available(_) | Free =>
        Success(copy(available = available - name,
          drop = drop + name))
      case Dropped =>
        Failure(new Error(s"Field $name already dropped"))
    }
  }

  def withoutAllFields(): Try[SchemaCalculator] = {
    Success(copy(available = Map(),
      drop = Set(),
      dropped = true))
  }
}

object SchemaCalculator {
  sealed trait FieldStatus
  object FieldStatus {
    case class Available(field: StructField) extends FieldStatus
    object Free extends FieldStatus
    object Dropped extends FieldStatus
  }
}