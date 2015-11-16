package org.apache.mleap.spark

import org.apache.mleap.runtime._
import org.apache.mleap.runtime.types.{StructField, StructType}
import org.apache.spark.sql.types

/**
  * Created by hwilkins on 11/12/15.
  */
case class SparkLeapFrame(schema: StructType,
                          sparkSchema: types.StructType,
                          dataset: SparkDataset,
                          outputFields: Set[String] = Set()) extends LeapFrame[SparkLeapFrame] {
  override def toLocal: LocalLeapFrame = LocalLeapFrame(schema, ArrayDataset(dataset.rdd.map(_._1).collect))

  override def select(fields: String*): SparkLeapFrame = {
    val indices = fields.map(schema.indexOf)
    val schema2 = schema.select(indices: _*)
    val dataset2 = dataset.map(_.select(indices: _*))

    SparkLeapFrame(schema2,
      sparkSchema,
      dataset2,
      outputFields = outputFields & fields.toSet)
  }

  def withFeature(field: StructField, f: (Row) => Any): SparkLeapFrame = {
    val schema2 = StructType(schema.fields :+ field)
    val dataset2 = dataset.map {
      row => row.withValue(f(row))
    }

    SparkLeapFrame(schema2,
      sparkSchema,
      dataset2,
      outputFields = outputFields + field.name)
  }
}
