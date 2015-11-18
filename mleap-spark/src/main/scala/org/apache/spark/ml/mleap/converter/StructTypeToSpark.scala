package org.apache.spark.ml.mleap.converter

import org.apache.mleap.runtime.types
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}

/**
  * Created by hwilkins on 11/18/15.
  */
case class StructTypeToSpark(schema: types.StructType) {
  def toSpark: StructType = {
    val fields = schema.fields.map {
      field =>
        field.dataType match {
          case types.DoubleType => StructField(field.name, DoubleType)
          case types.StringType => StructField(field.name, StringType)
          case types.VectorType => StructField(field.name, new VectorUDT())
        }
    }

    StructType(fields)
  }
}
