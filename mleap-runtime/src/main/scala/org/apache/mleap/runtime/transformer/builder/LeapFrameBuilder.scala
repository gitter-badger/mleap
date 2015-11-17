package org.apache.mleap.runtime.transformer.builder

import org.apache.mleap.runtime.{Row, LeapFrame}
import org.apache.mleap.runtime.types.{DataType, StructField}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
case class LeapFrameBuilder[T <: LeapFrame[T]](frame: T) extends TransformBuilder[LeapFrameBuilder[T]] {
  override def withInput(name: String, dataType: DataType): Try[(LeapFrameBuilder[T], Int)] = {
    frame.schema(name) match {
      case Some(field) =>
        if(field.dataType == dataType) {
          Success(this, frame.schema.indexOf(name).get)
        } else {
          Failure(new Error(s"Field $name expected data type ${field.dataType} but found $dataType"))
        }
      case None =>
        Failure(new Error(s"Field $name does not exist"))
    }
  }

  override def withOutput(name: String, dataType: DataType)
                         (o: (Row) => Any): Try[(LeapFrameBuilder[T], Int)] = {
    frame.withField(StructField(name, dataType), o).flatMap {
      frame2 => Success((copy(frame = frame2), frame2.schema.indexOf(name).get))
    }
  }

  override def withSelect(fieldNames: Seq[String]): Try[LeapFrameBuilder[T]] = {
    frame.select(fieldNames: _*).map(LeapFrameBuilder.apply)
  }

  override def withDrop(name: String): Try[LeapFrameBuilder[T]] = {
    frame.dropField(name).map(LeapFrameBuilder.apply)
  }
}
