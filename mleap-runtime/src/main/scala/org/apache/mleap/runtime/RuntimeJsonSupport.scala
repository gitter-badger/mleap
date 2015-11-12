package org.apache.mleap.runtime

import org.apache.mleap.core.CoreJsonSupport
import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.runtime.transformer._
import org.apache.mleap.runtime.types.{StructType, DataType, StructField}
import spray.json.DefaultJsonProtocol
import spray.json._
import scala.language.implicitConversions


/**
  * Created by hwilkins on 11/12/15.
  */
trait RuntimeJsonSupport extends DefaultJsonProtocol with CoreJsonSupport {
  // Types

  implicit object MleapDataTypeFormat extends JsonFormat[DataType] {
    override def write(obj: DataType): JsValue = JsString(obj.typeName)
    override def read(json: JsValue): DataType = DataType.fromName(json)
  }

  implicit object MleapStructFieldFormat extends RootJsonFormat[StructField] {
    override def write(obj: StructField): JsValue = {
      JsObject(Map("name" -> JsString(obj.name), "dataType" -> obj.dataType.toJson))
    }

    override def read(json: JsValue): StructField = {
      val obj = json.asJsObject()
      val name: String = obj.fields("name")

      StructField(name, obj.fields("dataType").convertTo[DataType])
    }
  }

  implicit val mleapStructTypeFormat = jsonFormat[Seq[StructField], StructType](StructType.apply, "fields")

  private[mleap] case class MleapTransformerFormat() extends RootJsonFormat[Transformer] {
    override def write(obj: Transformer): JsValue = obj match {
      case obj: LinearRegressionModel => obj.toJson
      case obj: OneHotEncoderModel => obj.toJson
      case obj: PipelineModel => obj.toJson
      case obj: RandomForestRegressionModel => obj.toJson
      case obj: StandardScalerModel => obj.toJson
      case obj: StringIndexerModel => obj.toJson
      case obj: VectorAssemblerModel => obj.toJson
    }

    override def read(json: JsValue): Transformer = {
      (json.asJsObject.fields("type"): String) match {
        case Transformer.linearRegressionModelName => json.convertTo[LinearRegressionModel]
        case Transformer.stringIndexerModelName => json.convertTo[StringIndexerModel]
      }
    }
  }

  implicit val mleapTransformerFormat = rootFormat(lazyFormat(MleapTransformerFormat()))

  private implicit val mleapLinearRegressionModelFormat = TypedFormat[LinearRegressionModel](Transformer.linearRegressionModelName, jsonFormat3(LinearRegressionModel))
  private implicit val mleapOneHotEncoderModelFormat = TypedFormat[OneHotEncoderModel](Transformer.oneHotEncoderModelName, jsonFormat3(OneHotEncoderModel))
  private implicit val mleapPipelineModelFormat = TypedFormat[PipelineModel](Transformer.pipelineModelName, jsonFormat1(PipelineModel))
  private implicit val mleapRandomForestRegressionModelFormat = TypedFormat[RandomForestRegressionModel](Transformer.randomForestRegressionModelName, jsonFormat3(RandomForestRegressionModel))
  private implicit val mleapStandardScalerModelFormat = TypedFormat[StandardScalerModel](Transformer.standardScalerModelName, jsonFormat3(StandardScalerModel))
  private implicit val mleapStringIndexerModelFormat = TypedFormat[StringIndexerModel](Transformer.stringIndexerModelName, jsonFormat3(StringIndexerModel))
  private implicit val mleapVectorAssemblerModelFormat = TypedFormat[VectorAssemblerModel](Transformer.vectorAssemblerModelName, jsonFormat[StructType, String, VectorAssemblerModel](VectorAssemblerModel, "inputSchema", "outputCol"))

  implicit object MleapRowFormat extends RootJsonFormat[Row] {
    override def write(obj: Row): JsValue = {
      val values = obj.data.map {
        case value: Double => JsNumber(value)
        case value: String => JsString(value)
        case value: Vector => value.toJson
      }

      JsArray(values: _*)
    }

    override def read(json: JsValue): Row = json match {
      case JsArray(values) =>
        val data = values.map {
          case JsNumber(value) => value.toDouble
          case JsString(value) => value
          case value => value.convertTo[Vector]
        }
        Row(data: _*)
      case value => throw new Error("Invalid JSON Row format: " + value)
    }
  }

  implicit val mleapArrayDatasetFormat = jsonFormat1(ArrayDataset)
  implicit val mleapLocalLeapFrameFormat = jsonFormat2(LocalLeapFrame)
}
object RuntimeJsonSupport extends RuntimeJsonSupport