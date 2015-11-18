package org.apache.spark.ml.mleap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.mleap.converter.MleapSparkSupport._
import org.apache.spark.ml.param.{Params, BooleanParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.ml.{Model, Estimator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.mleap.core

/**
 * Created by hwilkins on 11/8/15.
 */
// Original source from Spark here: https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/feature/OneHotEncoder.scala
private[mleap] trait OneHotEncoderBase extends Params with HasInputCol with HasOutputCol

class OneHotEncoder(override val uid: String)
  extends Estimator[OneHotEncoderModel]
  with OneHotEncoderBase {
  def this() = this(Identifiable.randomUID("mleapOneHot"))

  /**
   * Whether to drop the last category in the encoded vector (default: true)
   * @group param
   */
  final val dropLast: BooleanParam =
    new BooleanParam(this, "dropLast", "whether to drop the last category")
  setDefault(dropLast -> true)

  /** @group setParam */
  def setDropLast(value: Boolean): this.type = set(dropLast, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): OneHotEncoderModel = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    var outputAttrGroup = AttributeGroup.fromStructField(
      transformSchema(dataset.schema)(outputColName))
    if (outputAttrGroup.size < 0) {
      // If the number of attributes is unknown, we check the values from the input column.
      val numAttrs = dataset.select(col(inputColName).cast(DoubleType)).map(_.getDouble(0))
        .aggregate(0.0)(
          (m, x) => {
            assert(x >=0.0 && x == x.toInt,
              s"Values from column $inputColName must be indices, but got $x.")
            math.max(m, x)
          },
          (m0, m1) => {
            math.max(m0, m1)
          }
        ).toInt + 1
      val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
      val filtered = if ($(dropLast)) outputAttrNames.dropRight(1) else outputAttrNames
      val outputAttrs: Array[Attribute] =
        filtered.map(name => BinaryAttribute.defaultAttr.withName(name))
      outputAttrGroup = new AttributeGroup(outputColName, outputAttrs)
    }
    val size = outputAttrGroup.size

    copyValues(new OneHotEncoderModel(size))
  }

  override def copy(extra: ParamMap): Estimator[OneHotEncoderModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    SchemaUtils.checkColumnType(schema, inputColName, DoubleType)
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == outputColName),
      s"Output column $outputColName already exists.")

    val inputAttr = Attribute.fromStructField(schema(inputColName))
    val outputAttrNames: Option[Array[String]] = inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column $inputColName cannot be numeric.")
      case _ =>
        None // optimistic about unknown attributes
    }

    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if ($(dropLast)) {
        require(names.length > 1,
          s"The input column $inputColName should have at least two distinct values.")
        names.dropRight(1)
      } else {
        names
      }
    }

    val outputAttrGroup = if (filteredOutputAttrNames.isDefined) {
      val attrs: Array[Attribute] = filteredOutputAttrNames.get.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup($(outputCol), attrs)
    } else {
      new AttributeGroup($(outputCol))
    }

    val outputFields = inputFields :+ outputAttrGroup.toStructField()
    StructType(outputFields)
  }
}

class OneHotEncoderModel(override val uid: String,
                         size: Int)
  extends Model[OneHotEncoderModel]
  with OneHotEncoderBase {

  def this(size: Int) = this(Identifiable.randomUID("mleapOneHotModel"), size)

  val encoder: core.feature.OneHotEncoder = core.feature.OneHotEncoder(size)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  def getSize: Int = size

  override def copy(extra: ParamMap): OneHotEncoderModel = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    val encode = udf {
      (label: Double) =>
        assert(label >= 0.0 && label == label.toInt,
          s"Values from column $inputColName must be indices, but got $label.")

        encoder(label).toSpark
    }

    val attrs: Array[Attribute] = Array.tabulate(size) { n => BinaryAttribute.defaultAttr.withName(n.toString) }
    val outputAttrGroup = new AttributeGroup($(outputCol), attrs)
    val metadata = outputAttrGroup.toMetadata()

    dataset.select(col("*"), encode(col(inputColName)).as(outputColName, metadata))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    SchemaUtils.checkColumnType(schema, inputColName, DoubleType)
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == outputColName),
      s"Output column $outputColName already exists.")

    val fields = schema.fields
    val attrs: Array[Attribute] = Array.tabulate(size) { n => BinaryAttribute.defaultAttr.withName(n.toString) }
    val outputAttrGroup = new AttributeGroup($(outputCol), attrs)

    StructType(fields :+ outputAttrGroup.toStructField())
  }
}
