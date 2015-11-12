// Original code here: https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/feature/VectorAssembler.scala

package org.apache.spark.ml.mleap

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.attribute.{UnresolvedAttribute, AttributeGroup, NumericAttribute, Attribute}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCols}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Model, Estimator}
import org.apache.spark.mllib.linalg.{Vectors, Vector, VectorUDT}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuilder

class VectorAssembler(override val uid: String)
  extends Estimator[VectorAssemblerModel] with HasInputCols with HasOutputCol {
  def this() = this(Identifiable.randomUID("mleapVecAssembler"))

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): VectorAssemblerModel = {
    val inputColNames = $(inputCols)
    val inputFields = inputColNames.map(name => dataset.schema(name))
    val inputSchema = StructType(inputFields)

    copyValues(new VectorAssemblerModel(inputSchema))
  }

  override def copy(extra: ParamMap): Estimator[VectorAssemblerModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColName = $(outputCol)
    val inputDataTypes = inputColNames.map(name => schema(name).dataType)
    inputDataTypes.foreach {
      case _: NumericType | BooleanType =>
      case t if t.isInstanceOf[VectorUDT] =>
      case other =>
        throw new IllegalArgumentException(s"Data type $other is not supported.")
    }
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, new VectorUDT, true))
  }
}

class VectorAssemblerModel(override val uid: String,
                           inputSchema: StructType)
  extends Model[VectorAssemblerModel] with HasInputCols with HasOutputCol {
  def this(inputSchema: StructType) = this(Identifiable.randomUID("mleapVecAssemblerModel"), inputSchema)

  def getInputSchema: StructType = inputSchema

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def copy(extra: ParamMap): VectorAssemblerModel = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {
    val schema = dataset.schema
    validateSchema(schema)

    val assembleFunc = udf { r: Row =>
      VectorAssemblerModel.assemble(r.toSeq: _*)
    }
    val args = $(inputCols).map { c =>
      schema(c).dataType match {
        case DoubleType => dataset(c)
        case _: VectorUDT => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType).as(s"${c}_double_$uid")
      }
    }

    lazy val first = dataset.first()
    val attrs = $(inputCols).flatMap { c =>
      val field = schema(c)
      val index = schema.fieldIndex(c)
      field.dataType match {
        case DoubleType =>
          val attr = Attribute.fromStructField(field)
          // If the input column doesn't have ML attribute, assume numeric.
          if (attr == UnresolvedAttribute) {
            Some(NumericAttribute.defaultAttr.withName(c))
          } else {
            Some(attr.withName(c))
          }
        case _: NumericType | BooleanType =>
          // If the input column type is a compatible scalar type, assume numeric.
          Some(NumericAttribute.defaultAttr.withName(c))
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          if (group.attributes.isDefined) {
            // If attributes are defined, copy them with updated names.
            group.attributes.get.map { attr =>
              if (attr.name.isDefined) {
                // TODO: Define a rigorous naming scheme.
                attr.withName(c + "_" + attr.name.get)
              } else {
                attr
              }
            }
          } else {
            // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
            // from metadata, check the first row.
            val numAttrs = group.numAttributes.getOrElse(first.getAs[Vector](index).size)
            Array.fill(numAttrs)(NumericAttribute.defaultAttr)
          }
      }
    }
    val metadata = new AttributeGroup($(outputCol), attrs).toMetadata()

    dataset.select(col("*"), assembleFunc(struct(args : _*)).as($(outputCol), metadata))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)

    StructType(schema.fields :+ new StructField($(outputCol), new VectorUDT, true))
  }

  private def validateSchema(schema: StructType): Unit = {
    val inputFields = $(inputCols).map(name => (schema(name), inputSchema(name)))

    inputFields.foreach {
      case (datasetField, inputField) =>
        datasetField.dataType match {
          case _: NumericType | BooleanType =>
            inputField.dataType match {
              case _: NumericType | BooleanType => true
              case _ => throw new SparkException("field in dataset does not match expected input")
            }
          case _: VectorUDT =>
            inputField.dataType match {
              case _: VectorUDT => true
              case _ => throw new SparkException("field in dataset does not match expected input")
            }
        }
    }
  }
}

private object VectorAssemblerModel {
  private[mleap] def assemble(vv: Any*): Vector = {
    val indices = ArrayBuilder.make[Int]
    val values = ArrayBuilder.make[Double]
    var cur = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
          indices += cur
          values += v
        }
        cur += 1
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }
        cur += vec.size
      case null =>
        // TODO: output Double.NaN?
        throw new SparkException("Values to assemble cannot be null.")
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }
    Vectors.sparse(cur, indices.result(), values.result()).compressed
  }
}
