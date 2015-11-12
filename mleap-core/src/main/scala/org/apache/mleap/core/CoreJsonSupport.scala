package org.apache.mleap.core

import org.apache.mleap.core.feature.{VectorAssembler, StandardScaler, OneHotEncoder, StringIndexer}
import org.apache.mleap.core.linalg.{SparseVector, Vector, DenseVector}
import org.apache.mleap.core.regression.{RandomForestRegression, DecisionTreeRegression, LinearRegression}
import org.apache.mleap.core.tree._
import spray.json.DefaultJsonProtocol
import spray.json._
import scala.language.implicitConversions

/**
  * Created by hwilkins on 11/12/15.
  */
trait CoreJsonSupport extends DefaultJsonProtocol {
  // Utility Methods

  private[mleap] implicit def extractString(json: JsValue): String = json match {
    case JsString(value) => value
    case value => throw new Error("invalid string: " + value)
  }

  private[mleap] implicit def extractInt(json: JsValue): Int = json match {
    case JsNumber(value) => value.toInt
    case value => throw new Error("invalid string: " + value)
  }

  private[mleap] implicit def extractDouble(json: JsValue): Double = json match {
    case JsNumber(value) => value.toDouble
    case value => throw new Error("invalid string: " + value)
  }

  private[mleap] implicit def extractIntArray(json: JsValue): Array[Int] = json match {
    case JsArray(values) => values.map(value => extractInt(value)).toArray
    case value => throw new Error("invalid string: " + value)
  }

  private[mleap] implicit def extractDoubleArray(json: JsValue): Array[Double] = json match {
    case JsArray(values) => values.map(value => extractDouble(value)).toArray
    case value => throw new Error("invalid string: " + value)
  }

  private[mleap] case class TypedFormat[T](tpe: String,
                            baseFormat: RootJsonFormat[T]) extends RootJsonFormat[T] {
    override def write(obj: T): JsValue = {
      JsObject(baseFormat.write(obj).asJsObject.fields + ("type" -> JsString(tpe)))
    }

    override def read(json: JsValue): T = baseFormat.read(json)
  }

  // Linear Algebra Types

  implicit object MleapVectorFormat extends JsonFormat[Vector] {
    override def write(obj: Vector): JsValue = obj match {
      case DenseVector(values) =>
        val jsValues = values.map(JsNumber(_))
        JsArray(jsValues: _*)
      case SparseVector(size, indices, values) =>
        val jsSize = JsNumber(size)
        val jsIndices = JsArray(indices.map(JsNumber(_)): _*)
        val jsValues = JsArray(values.map(JsNumber(_)): _*)
        JsObject(Map("size" -> jsSize, "indices" -> jsIndices, "values" -> jsValues))
    }

    override def read(json: JsValue): Vector = json match {
      case jsValues: JsArray =>
        val values: Array[Double] = jsValues
        DenseVector(values)
      case JsObject(fields) =>
        val size: Int = fields("size")
        val indices: Array[Int] = fields("indices")
        val values: Array[Double] = fields("values")
        SparseVector(size, indices, values)
      case _ => throw new Error("invalid JSON Vector format")
    }
  }

  // Tree Types

  private implicit val mleapCategoricalSplitFormat = TypedFormat[CategoricalSplit](Split.categoricalSplitName, jsonFormat3(CategoricalSplit))
  private implicit val mleapContinuousSplitFormat = TypedFormat[ContinuousSplit](Split.continuousSplitName, jsonFormat2(ContinuousSplit))

  implicit object MleapSplitFormat extends RootJsonFormat[Split] {
    override def write(obj: Split): JsValue = obj match {
      case obj: CategoricalSplit => obj.toJson
      case obj: ContinuousSplit => obj.toJson
    }

    override def read(json: JsValue): Split = {
      (json.asJsObject.fields("type"): String) match {
        case Split.categoricalSplitName => json.convertTo[CategoricalSplit]
        case Split.continuousSplitName => json.convertTo[ContinuousSplit]
      }
    }
  }

  private[mleap] case class MleapNodeFormat() extends RootJsonFormat[Node] {
    override def write(obj: Node): JsValue = obj match {
      case obj: InternalNode => obj.toJson
      case obj: LeafNode => obj.toJson
    }

    override def read(json: JsValue): Node = {
      (json.asJsObject.fields("type"): String) match {
        case Node.internalNodeName => json.convertTo[InternalNode]
        case Node.leafNodeName => json.convertTo[LeafNode]
      }
    }
  }

  implicit val mleapNodeFormat = rootFormat(lazyFormat(MleapNodeFormat()))

  private implicit val mleapInternalNodeFormat = TypedFormat[InternalNode](Node.internalNodeName, jsonFormat6(InternalNode))
  private implicit val mleapLeafNodeFormat = TypedFormat[LeafNode](Node.leafNodeName, jsonFormat2(LeafNode))

  // Feature Types

  implicit val mleapOneHotEncoderFormat = jsonFormat[Int, OneHotEncoder](OneHotEncoder, "size")
  implicit val mleapStandardScalerFormat = jsonFormat2(StandardScaler)
  implicit val mleapStringIndexerFormat = jsonFormat1(StringIndexer)
  implicit val mleapVectorAssemblerFormat = jsonFormat0(VectorAssembler.apply)

  // Regression Types

  implicit val mleapLinearRegressionFormat = jsonFormat2(LinearRegression)
  implicit val mleapDecisionTreeRegressionFormat = jsonFormat1(DecisionTreeRegression)
  implicit val mleapRandomForestRegressionFormat =
    jsonFormat[Seq[DecisionTreeRegression], Seq[Double], RandomForestRegression](RandomForestRegression, "trees", "treeWeights")
}
object CoreJsonSupport extends CoreJsonSupport
