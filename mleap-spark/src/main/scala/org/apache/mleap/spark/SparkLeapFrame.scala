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

    copy(schema = schema2, dataset = dataset2, outputFields = outputFields & fields.toSet)
  }

  def withFeature(field: StructField, f: (Row) => Any): SparkLeapFrame = {
    val schema2 = StructType(schema.fields :+ field)
    val dataset2 = dataset.map {
      row => row.withValue(f(row))
    }

    copy(schema = schema2, dataset = dataset2, outputFields = outputFields + field.name)
  }

  override def dropFeature(field: String): SparkLeapFrame = {
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

    copy(schema = schema2, dataset = dataset2, outputFields = outputFields - field)
  }
}
