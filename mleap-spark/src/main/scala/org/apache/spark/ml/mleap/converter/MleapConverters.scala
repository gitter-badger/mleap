package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core
import org.apache.mleap.core.linalg.{DenseVector => MleapDenseVector, SparseVector => MleapSparseVector, Vector => MleapVector}
import org.apache.mleap.runtime.{LeapFrame, Row, Transformer => MleapTransformer, transformer => tform, types}
import org.apache.mleap.spark.{SparkLeapFrame, SparkDataset}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.mleap.{VectorAssemblerModel, OneHotEncoderModel}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, DecisionTreeRegressionModel, LinearRegressionModel}
import org.apache.spark.ml.tree._
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.DataFrame
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
          case _: org.apache.spark.mllib.linalg.VectorUDT => types.VectorType
          case _ => throw new SparkException(s"unsupported MLeap datatype: $sparkTypeName")
        }

        types.StructField(field.name, dataType)
    }
    types.StructType(leapFields)
  }
}

case class DataFrameToMleap(dataset: DataFrame) {
  def toMleap: LeapFrame = {
    val schema = dataset.schema

    // cast all numeric types to Doubles
    val cols = schema.fields.map {
      field =>
        field.dataType match {
          case _: NumericType | BooleanType => dataset.col(field.name).cast(DoubleType).as(field.name)
          case _ => dataset.col(field.name)
        }
    }
    val castDataset = dataset.select(cols: _*)

    val leapRDD = castDataset.map {
      row =>
        val values = row.toSeq.toArray.map {
          case value: Vector => value.toMleap
          case value: Double => value
          case value: String => value
        }
        Row(values: _*)
    }
    val leapDataset = SparkDataset(leapRDD)
    SparkLeapFrame(schema.toMleap, leapDataset)
  }
}
