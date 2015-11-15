package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.feature.VectorAssembler
import org.apache.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.mleap.runtime.types.{StructType, VectorType, StructField}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class VectorAssemblerModel(inputSchema: StructType,
                                outputCol: String) extends Transformer {
  val assembler: VectorAssembler = VectorAssembler.default

  override def transform[T <: TransformBuilder[T]](builder: T): Try[T] = {
    inputSchema.fields.foldLeft(Try((builder, Seq[Int]()))) {
      (result, field) => result.flatMap {
        case(b2, indices) =>
          b2.withInput(field.name, field.dataType)
            .map {
              case (b3, index) => (b3, indices :+ index)
            }
      }
    }.flatMap {
      case (b, indices) =>
        b.endWithOutput(outputCol, VectorType)(row => assembler(indices.map(row.get): _*))
    }
  }
}
