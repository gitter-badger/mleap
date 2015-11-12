package org.apache.mleap.runtime.types

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

  def apply(name: String): StructField = nameToField(name)
  def indexOf(name: String): Int = nameToIndex(name)
  def contains(name: String): Boolean = nameToIndex.contains(name)

  def select(indices: Int *): StructType = {
    val selectedFields = indices.map(fields)
    StructType(selectedFields)
  }
}
