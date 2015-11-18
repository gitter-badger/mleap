package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core.linalg.{Vector => MleapVector}
import org.apache.mleap.runtime.{LeapFrame, Row => MleapRow, Transformer => MleapTransformer, types}
import org.apache.mleap.spark.SparkLeapFrame
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.Transformer
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types._

/**
  * Created by hwilkins on 11/5/15.
  */
trait MleapSparkSupport {
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

  implicit def mleapTransformerWrapper(transformer: MleapTransformer): MleapTransformerWrapper = MleapTransformerWrapper(transformer)
}
object MleapSparkSupport extends MleapSparkSupport
