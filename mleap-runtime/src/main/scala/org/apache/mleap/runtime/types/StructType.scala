package org.apache.mleap.runtime.types

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 10/23/15.
  */
object StructType {
  val empty = StructType(Seq())

  def withFields(fields: StructField *): StructType = StructType(fields.toSeq)
}

case class StructType(fields: Seq[StructField]) {
  val nameToIndex: Map[String, Int] = fields.map(_.name).zipWithIndex.toMap
  val nameToField: Map[String, StructField] = fields.map(_.name).zip(fields).toMap

  def apply(name: String): Option[StructField] = nameToField.get(name)
  def indexOf(name: String): Option[Int] = nameToIndex.get(name)
  def contains(name: String): Boolean = nameToIndex.contains(name)

  def withField(field: StructField): StructType = StructType(fields :+ field)

  def select(fieldNames: String *): Try[StructType] = {
    tryIndicesOf(fieldNames: _*).map(selectIndices(_: _*))
  }

  def selectIndices(indices: Int *): StructType = {
    StructType(indices.map(fields))
  }

  def tryIndicesOf(fieldNames: String *): Try[Seq[Int]] = {
    fieldNames.foldLeft(Try(Seq[Int]())) {
      (tryIndices, name) =>
        tryIndices.flatMap {
          indices =>
            indexOf(name) match {
              case Some(index) => Success(indices :+ index)
              case None => Failure(new Error(s"Field $name does not exist"))
            }
        }
    }
  }

  def dropField(name: String): Try[StructType] = {
    tryIndexOf(name).map(dropIndex)
  }

  def dropIndex(index: Int): StructType = {
    StructType(fields.zipWithIndex.filter(_._2 != index).map(_._1))
  }

  def tryIndexOf(name: String): Try[Int] = {
    indexOf(name) match {
      case Some(index) => Success(index)
      case None => Failure(new Error(s"Field $name does not exist"))
    }
  }
}
