package org.apache.mleap.spark

import org.apache.mleap.runtime._
import org.apache.mleap.runtime.types.{StructField, StructType}
import org.apache.spark.sql.types

import scala.util.{Success, Try}

/**
  * Created by hwilkins on 11/12/15.
  */
case class SparkLeapFrame(schema: StructType,
                          sparkSchema: types.StructType,
                          dataset: SparkDataset,
                          outputFields: Set[String] = Set()) extends LeapFrame[SparkLeapFrame] {
  override type D = SparkDataset

  override def toLocal: LocalLeapFrame = LocalLeapFrame(schema, ArrayDataset(dataset.rdd.map(_._1).collect))

  override protected def withFieldInternal(schema2: StructType,
                                           dataset2: D,
                                           field: StructField): Try[SparkLeapFrame] = {
    Success(copy(schema = schema2, dataset = dataset2, outputFields = outputFields + field.name))
  }

  override protected def selectInternal(schema2: StructType,
                                        dataset2: D,
                                        fieldNames: String *): Try[SparkLeapFrame] = {
    Success(copy(schema = schema2, dataset = dataset2, outputFields = outputFields & fieldNames.toSet))
  }

  override protected def dropFieldInternal(schema2: StructType,
                                           dataset2: D,
                                           fieldName: String): Try[SparkLeapFrame] = {
    Success(copy(schema = schema2, dataset = dataset2, outputFields = outputFields - fieldName))
  }
}
