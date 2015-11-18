package org.apache.spark.ml.mleap.converter

import org.apache.mleap.runtime.types
import org.apache.mleap.spark.{SparkDataset, VectorUDT => MleapVectorUDT, SparkLeapFrame}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, BooleanType, NumericType}
import org.apache.mleap.runtime.{Row => MleapRow}

/**
  * Created by hwilkins on 11/18/15.
  */
case class DataFrameToMleap(dataset: DataFrame) {
  def toMleap: SparkLeapFrame = {
    val mleapFields = dataset.schema.fields.flatMap {
      field =>
        field.dataType match {
          case _: NumericType | BooleanType | StringType => Seq(types.StructField(field.name, types.DoubleType))
          case _: VectorUDT => Seq(types.StructField(field.name, types.VectorType))
          case _: StringType => Seq(types.StructField(field.name, types.StringType))
          case _ => Seq()
        }
    }

    toMleap(types.StructType(mleapFields))
  }

  def toMleap(schema: types.StructType): SparkLeapFrame = {
    val sparkSchema = dataset.schema

    // cast MLeap field numeric types to DoubleTypes
    val mleapCols = schema.fields.map {
      field =>
        field.dataType match {
          case types.DoubleType => dataset.col(field.name).cast(DoubleType).as(s"mleap.${field.name}")
          case types.StringType => dataset.col(field.name).cast(StringType).as(s"mleap.${field.name}")
          case types.VectorType => dataset.col(field.name).cast(new MleapVectorUDT()).as(s"mleap.${field.name}")
        }
    }
    val cols = Seq(dataset.col("*")) ++ mleapCols
    val castDataset = dataset.select(cols: _*)

    val sparkIndices: Seq[Int] = sparkSchema.fields.indices
    val mleapIndices = (sparkSchema.fields.length until (sparkSchema.fields.length + schema.fields.length)).toArray

    val rdd = castDataset.rdd.map {
      row =>
        // finish converting Spark data structure to MLeap
        // TODO: make a Spark UDT for MleapVector and just
        // cast like we do for numeric types
        val mleapValues = mleapIndices.map(row.get)
        val mleapRow = MleapRow(mleapValues)
        val sparkRow = sparkIndices.map(row.apply)
        (mleapRow, sparkRow)
    }

    val mleapDataset = SparkDataset(rdd)
    SparkLeapFrame(schema, sparkSchema, mleapDataset)
  }
}