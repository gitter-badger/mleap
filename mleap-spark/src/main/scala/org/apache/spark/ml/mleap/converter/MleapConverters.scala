package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core
import org.apache.mleap.core.linalg.{DenseVector => MleapDenseVector, SparseVector => MleapSparseVector, Vector => MleapVector}
import org.apache.mleap.runtime.{LeapFrame, Row => MleapRow, Transformer => MleapTransformer, transformer => tform, types}
import org.apache.mleap.spark.{SparkLeapFrame, SparkDataset}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.mleap.{VectorAssemblerModel, OneHotEncoderModel}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, DecisionTreeRegressionModel, LinearRegressionModel}
import org.apache.spark.ml.tree._
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.types._

/**
  * Created by hwilkins on 11/5/15.
  */
object MleapConverters {
  import scala.language.implicitConversions

  implicit def vectorToSpark(vector: MleapVector): VectorToSpark = VectorToSpark(vector)
  implicit def vectorToMleap(vector: Vector): VectorToMleap = VectorToMleap(vector)
  implicit def dataFrameToMleap(dataset: DataFrame): DataFrameToMleap = DataFrameToMleap(dataset)
  implicit def decisionTreeRegressionModelToMleap(tree: DecisionTreeRegressionModel): DecisionTreeRegressionModelToMleap = DecisionTreeRegressionModelToMleap(tree)
  implicit def nodeToMleap(node: Node): NodeToMleap = NodeToMleap(node)
  implicit def splitToMleap(split: Split): SplitToMleap = SplitToMleap(split)
  implicit def transformerToMleap(transformer: Transformer): TransformerToMleap = TransformerToMleap(transformer)
  implicit def structTypeToMleap(schema: StructType): StructTypeToMleap = StructTypeToMleap(schema)

  implicit def rowToSpark(row: MleapRow): RowToSpark = RowToSpark(row)
  implicit def structTypeToSpark(schema: types.StructType): StructTypeToSpark = StructTypeToSpark(schema)
  implicit def leapFrameToSpark[T <: LeapFrame[T]](frame: T): LeapFrameToSpark[T] = LeapFrameToSpark[T](frame)

  implicit def leapFrameToSparkConvert[T <: LeapFrame[T]](frame: T)
                                                         (implicit sqlContext: SQLContext): DataFrame = frame.toSpark
  implicit def dataFrameToLeapFrame(dataFrame: DataFrame): SparkLeapFrame = dataFrame.toMleap
}

import MleapConverters._

case class VectorToSpark(vector: MleapVector) {
  def toSpark: Vector = vector match {
    case MleapDenseVector(values) => Vectors.dense(values)
    case MleapSparseVector(size, indices, values) => Vectors.sparse(size, indices, values)
  }
}

case class VectorToMleap(vector: Vector) {
  def toMleap: MleapVector = {
    vector match {
      case DenseVector(values) => MleapVector.dense(values)
      case SparseVector(size, indices, values) => MleapVector.sparse(size, indices, values)
    }
  }
}

case class TransformerToMleap(transformer: Transformer) {
  def toMleap: MleapTransformer = {
    transformer match {
      case transformer: LinearRegressionModel => tform.LinearRegressionModel(transformer.getFeaturesCol,
        transformer.getPredictionCol,
        core.regression.LinearRegression(transformer.weights.toMleap,
          transformer.intercept))
      case transformer: StandardScalerModel =>
        tform.StandardScalerModel(transformer.getInputCol,
          transformer.getOutputCol,
          core.feature.StandardScaler(Option(transformer.std.toMleap),
            Option(transformer.mean.toMleap)))
      case transformer: org.apache.spark.ml.mleap.StringIndexerModel =>
        tform.StringIndexerModel(transformer.getInputCol,
          transformer.getOutputCol,
          core.feature.StringIndexer(transformer.getLabels))
      case transformer: VectorAssemblerModel =>
        tform.VectorAssemblerModel(transformer.getInputSchema.toMleap,
          transformer.getOutputCol)
      case transformer: OneHotEncoderModel => tform.OneHotEncoderModel(transformer.getInputCol,
        transformer.getOutputCol,
        core.feature.OneHotEncoder(transformer.getSize))
      case transformer: PipelineModel => tform.PipelineModel(transformer.stages.map(_.toMleap))
      case transformer: RandomForestRegressionModel => tform.RandomForestRegressionModel(transformer.getFeaturesCol,
        transformer.getPredictionCol,
        RandomForestRegressionModelToMleap(transformer).toMleap)
    }
  }
}

case class RandomForestRegressionModelToMleap(forest: RandomForestRegressionModel) {
  def toMleap: core.regression.RandomForestRegression = {
    core.regression.RandomForestRegression(forest.trees.asInstanceOf[Array[DecisionTreeRegressionModel]].map(_.toMleap), forest.treeWeights)
  }
}

case class DecisionTreeRegressionModelToMleap(tree: DecisionTreeRegressionModel) {
  def toMleap: core.regression.DecisionTreeRegression = {
    core.regression.DecisionTreeRegression(tree.rootNode.toMleap)
  }
}

case class NodeToMleap(node: Node) {
  def toMleap: core.tree.Node = {
    node match {
      case node: InternalNode =>
        core.tree.InternalNode(node.prediction,
          node.impurity,
          node.gain,
          node.leftChild.toMleap,
          node.rightChild.toMleap,
          node.split.toMleap)
      case node: LeafNode =>
        core.tree.LeafNode(node.prediction, node.impurity)
    }
  }
}

case class SplitToMleap(split: Split) {
  def toMleap: core.tree.Split = {
    split match {
      case split: CategoricalSplit =>
        val (isLeft, categories) = if(split.leftCategories.length >= split.rightCategories.length) {
          (true, split.leftCategories)
        } else {
          (false, split.rightCategories)
        }
        core.tree.CategoricalSplit(split.featureIndex, categories, isLeft)
      case split: ContinuousSplit =>
        core.tree.ContinuousSplit(split.featureIndex, split.threshold)
    }
  }
}

case class StructTypeToMleap(schema: StructType) {
  def toMleap: types.StructType = {
    val leapFields = schema.fields.map {
      field =>
        val sparkType = field.dataType
        val sparkTypeName = sparkType.typeName
        val dataType = sparkType match {
          case _: NumericType | BooleanType => types.DoubleType
          case _: StringType => types.StringType
          case _: VectorUDT => types.VectorType
          case _ => throw new SparkException(s"unsupported MLeap datatype: $sparkTypeName")
        }

        types.StructField(field.name, dataType)
    }
    types.StructType(leapFields)
  }
}

case class DataFrameToMleap(dataset: DataFrame) {
  def toMleap: SparkLeapFrame = {
    val mleapFields = dataset.schema.fields.filter {
      field =>
        field.dataType match {
          case _: NumericType | BooleanType | StringType => true
          case _: VectorUDT => true
          case _ => false
        }
    }

    toMleap(mleapFields.map(_.name): _ *)
  }

  def toMleap(fieldNames: String *): SparkLeapFrame = {
    val fieldSet = fieldNames.toSet
    val allFieldSet = dataset.schema.fields.map(_.name).toSet

    val mleapFieldSet = allFieldSet & fieldSet
    val mleapFields = mleapFieldSet.map(dataset.schema.apply).toArray

    val sparkSchema = dataset.schema
    val mleapSchema = StructType(mleapFields).toMleap

    // cast MLeap field numeric types to DoubleTypes
    val mleapCols = mleapFields.map {
      field =>
        field.dataType match {
          case _: NumericType | BooleanType => dataset.col(field.name).cast(DoubleType).as(s"mleap.${field.name}")
          case _ => dataset.col(field.name).as(s"mleap.${field.name}")
        }
    }
    val castDataset = dataset.select(dataset.col("*") +: mleapCols: _*)

    val sparkIndices: Seq[Int] = sparkSchema.fields.indices
    val mleapIndices = (sparkSchema.fields.length until (sparkSchema.fields.length + mleapSchema.fields.length)).toArray

    val rdd = castDataset.rdd.map {
      row =>
        // finish converting Spark data structure to MLeap
        // TODO: make a Spark UDT for MleapVector and just
        // cast like we do for numeric types
        val mleapValues = mleapIndices.map(row.get).map {
          case value: Vector => value.toMleap
          case value => value
        }
        val mleapRow = MleapRow(mleapValues)
        val sparkRow = sparkIndices.map(row.apply)
        (mleapRow, sparkRow)
    }

    val mleapDataset = SparkDataset(rdd)
    SparkLeapFrame(mleapSchema, sparkSchema, mleapDataset)
  }
}

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

case class RowToSpark(row: MleapRow) {
  def toSpark: Row = {
    val values = row.toArray.map {
      case value: MleapVector => value.toSpark
      case value => value
    }

    Row(values: _*)
  }
}

case class LeapFrameToSpark[T <: LeapFrame[T]](frame: LeapFrame[T]) {
  def toSpark(implicit sqlContext: SQLContext): DataFrame = frame match {
    case frame: SparkLeapFrame =>
      val outputFrame = frame.select(frame.outputFields.toSeq: _*)
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
