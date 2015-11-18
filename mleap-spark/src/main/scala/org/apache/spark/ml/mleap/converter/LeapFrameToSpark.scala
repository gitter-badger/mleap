package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core.linalg.{Vector => MleapVector}
import org.apache.mleap.runtime.LeapFrame
import org.apache.mleap.spark.SparkLeapFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import MleapSparkSupport._

/**
  * Created by hwilkins on 11/18/15.
  */
case class LeapFrameToSpark[T <: LeapFrame[T]](frame: T) {
  def toSpark(implicit sqlContext: SQLContext): DataFrame = frame match {
    case frame: SparkLeapFrame =>
      // must explicitly cast or there are compile errors
      val frame2 = frame.asInstanceOf[SparkLeapFrame]
      val outputFrame = frame2.select(frame2.outputFields.toSeq: _*).get
      val rows = outputFrame.dataset.rdd.map {
        case (mleapRow, sparkData) =>
          val mleapData = mleapRow.toArray.map {
            case value: MleapVector => value.toSpark
            case value => value
          }

          Row(sparkData ++ mleapData: _*)
      }

      val schema = StructType(outputFrame.sparkSchema.fields ++ outputFrame.schema.toSpark.fields)
      sqlContext.createDataFrame(rows, schema)
    case _ =>
      val schema = frame.schema.toSpark
      val rows = frame.dataset.toArray.map(_.toSpark)
      val rdd = sqlContext.sparkContext.parallelize(rows)
      sqlContext.createDataFrame(rdd, schema)
  }
}
