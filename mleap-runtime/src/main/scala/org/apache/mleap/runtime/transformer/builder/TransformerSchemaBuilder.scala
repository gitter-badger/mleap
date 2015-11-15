package org.apache.mleap.runtime.transformer.builder

import org.apache.mleap.runtime.Row
import org.apache.mleap.runtime.types.{StructType, DataType, StructField}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
case class TransformerSchemaBuilder(schema: StructType = StructType.empty,
                                    input: Map[String, StructField] = Map(),
                                    output: Map[String, StructField] = Map()) extends TransformBuilder[TransformerSchemaBuilder] {
  override def validateField(name: String, dataType: DataType): Validation = {
    if(schema.contains(name)) {
      val otherDataType = schema(name).dataType
      if(dataType == otherDataType) {
        Valid
      } else {
        Invalid(otherDataType)
      }
    } else {
      Valid
    }
  }
  override def hasField(name: String): Boolean = available.contains(name)

  override protected def withInputInternal(name: String,
                                           dataType: DataType): Try[(TransformerSchemaBuilder, Int)] = {
    if(schema.contains(name)) {
      Success(this, schema.indexOf(name))
    } else {
      val field = StructField(name, dataType)
      val schema2 = StructType(schema.fields :+ field)
      Success(copy(schema = schema2,
        input = input + (name -> field)),
        schema2.indexOf(name))
    }
  }

  override protected def withOutputInternal(name: String,
                                            dataType: DataType)
                                           (o: (Row) => Any): Try[(TransformerSchemaBuilder, Int)] = {
    if(schema.contains(name)) {
      Failure(new Error(s"Field $name is already in LeapFrame"))
    } else {
      val field = StructField(name, dataType)
      val schema2 = StructType(schema.fields :+ field)
      Success(copy(schema = schema2,
        output = output + (name -> field)),
        schema2.indexOf(name))
    }
  }
}
