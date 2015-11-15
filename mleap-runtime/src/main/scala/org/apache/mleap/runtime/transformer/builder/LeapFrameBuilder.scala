package org.apache.mleap.runtime.transformer.builder

import org.apache.mleap.runtime.{Row, LeapFrame}
import org.apache.mleap.runtime.types.{StructType, DataType, StructField}

import scala.util.{Success, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
case class LeapFrameBuilder(frame: LeapFrame) extends TransformBuilder[LeapFrameBuilder] {
  override def validateField(name: String, dataType: DataType): Validation = {
    val otherDataType = frame.schema(name).dataType
    if(dataType == otherDataType) {
      Valid
    } else {
      Invalid(otherDataType)
    }
  }

  override def hasField(name: String): Boolean = frame.schema.contains(name)

  override protected def withInputInternal(name: String,
                                           dataType: DataType): Try[(LeapFrameBuilder, Int)] = {
    Success((this, frame.schema.indexOf(name)))
  }

  override protected def withOutputInternal(name: String,
                                            dataType: DataType)
                                           (o: (Row) => Any): Try[(LeapFrameBuilder, Int)] = {
    val frame2 = frame.withFeature(StructField(name, dataType), o)
    Success((LeapFrameBuilder(frame2), frame2.schema.indexOf(name)))
  }

  override def withSelectInternal(schema: StructType): Try[LeapFrameBuilder] = {
    Success(LeapFrameBuilder(frame.select(schema.fields.map(_.name): _*)))
  }
}
