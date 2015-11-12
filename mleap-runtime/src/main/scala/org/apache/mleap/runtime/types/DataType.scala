package org.apache.mleap.runtime.types

import DataType._

/**
 * Created by hwilkins on 10/23/15.
 */
object DataType {
  val doubleTypeName = "double"
  val stringTypeName = "string"
  val vectorTypeName = "vector"

  def fromName(name: String): DataType = name match {
    case `doubleTypeName` => DoubleType
    case `stringTypeName` => StringType
    case `vectorTypeName` => VectorType
  }
}

sealed trait DataType extends Serializable {
  def typeName: String
}

object DoubleType extends DataType {
  override def typeName: String = doubleTypeName
}

object StringType extends DataType {
  override def typeName: String = stringTypeName
}

object VectorType extends DataType {
  override def typeName: String = vectorTypeName
}
