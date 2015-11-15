package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.core.regression.RandomForestRegression
import org.apache.mleap.runtime.types.{VectorType, DoubleType, StructField}
import org.apache.mleap.runtime._

import scala.util.Try

/**
  * Created by hwilkins on 11/8/15.
  */
case class RandomForestRegressionModel(featuresCol: String,
                                       predictionCol: String,
                                       model: RandomForestRegression) extends Transformer {
  override def calculateSchema(calc: SchemaCalculator): Try[SchemaCalculator] = {
    calc.withInputField(featuresCol, VectorType)
      .flatMap(_.withOutputField(predictionCol, DoubleType))
  }

  override def transform(dataset: LeapFrame): LeapFrame = {
    val featuresIndex = dataset.schema.indexOf(featuresCol)
    val predict = {
      (row: Row) =>
        model.predict(row.getAs[Vector](featuresIndex))
    }

    dataset.withFeature(StructField(predictionCol, DoubleType), predict)
  }
}
