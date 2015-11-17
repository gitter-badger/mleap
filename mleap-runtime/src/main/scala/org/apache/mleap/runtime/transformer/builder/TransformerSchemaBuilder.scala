package org.apache.mleap.runtime.transformer.builder

import org.apache.mleap.runtime.{TransformerSchema, Row}
import org.apache.mleap.runtime.types.{StructType, DataType, StructField}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
case class TransformerSchemaBuilder(schema: StructType = StructType.empty,
                                    input: Map[String, StructField] = Map(),
                                    drops: Set[String] = Set(),
                                    selected: Boolean = false) extends TransformBuilder[TransformerSchemaBuilder] {
  def build(): TransformerSchema = {
    val inputSchema = StructType(input.values.toSeq)

    TransformerSchema(inputSchema, schema)
  }

  override def withInput(name: String, dataType: DataType): Try[(TransformerSchemaBuilder, Int)] = {
    if(schema.contains(name)) {
      Success(this, schema.indexOf(name).get)
    } else if(selected || drops.contains(name)) {
      Failure(new Error(s"Field $name was dropped"))
    } else {
      val field = StructField(name, dataType)
      val schema2 = StructType(schema.fields :+ field)
      val input2 = input + (name -> field)
      val index = schema.fields.length
      val builder = copy(schema = schema2, input = input2)

      Success(builder, index)
    }
  }

  override def withOutput(name: String, dataType: DataType)
                         (o: (Row) => Any): Try[(TransformerSchemaBuilder, Int)] = {
    schema.indexOf(name) match {
      case Some(index) =>
        Failure(new Error(s"Field $name already exists"))
      case None =>
        val field = StructField(name, dataType)
        val schema2 = StructType(schema.fields :+ field)
        val drops2 = drops - name
        val builder = copy(schema = schema2, drops = drops2)
        val index = schema.fields.length

        Success(builder, index)
    }
  }

  override def withSelect(fieldNames: Seq[String]): Try[TransformerSchemaBuilder] = {
    schema.select(fieldNames: _*).map {
      schema2 =>
        TransformerSchemaBuilder(schema = schema2, input = input, selected = true)
    }
  }

  override def withDrop(name: String): Try[TransformerSchemaBuilder] = {
    schema.dropField(name).map {
      schema2 =>
        val drops2 = drops + name

        copy(schema = schema2, drops = drops2)
    }
  }
}
