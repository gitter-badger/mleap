package org.apache.mleap.runtime.transformer.builder

import org.apache.mleap.runtime.types.{StructType, DataType}
import org.apache.mleap.runtime.Row

import scala.util.{Failure, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
trait TransformBuilder[T <: TransformBuilder[T]] {
  def withInput(name: String, dataType: DataType): Try[(T, Int)]
  def endWithInput(name: String, dataType: DataType): Try[T] = withInput(name, dataType).map(_._1)

  def withOutput(name: String, dataType: DataType)
                (o: (Row) => Any): Try[(T, Int)]
  def endWithOutput(name: String, dataType: DataType)
                   (o: (Row) => Any): Try[T] = withOutput(name, dataType)(o).map(_._1)

  def withSelect(fieldNames: Seq[String]): Try[T]
  def withDrop(name: String): Try[T]
}
