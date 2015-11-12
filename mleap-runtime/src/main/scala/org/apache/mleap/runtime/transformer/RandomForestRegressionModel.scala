package org.apache.mleap.runtime.transformer

import org.apache.mleap.core.linalg.Vector
import org.apache.mleap.core.regression.RandomForestRegression
import org.apache.mleap.runtime.types.{StructType, VectorType, DoubleType, StructField}
import org.apache.mleap.runtime.{Row, LeapFrame, Transformer}

/**
 * Created by hwilkins on 11/8/15.
 */
case class RandomForestRegressionModel(featuresCol: String,
                                       predictionCol: String,
                                       model: RandomForestRegression) extends Transformer {
  override def inputSchema: StructType = StructType.withFields(StructField(featuresCol, VectorType))

  override def transform(dataset: LeapFrame): LeapFrame = {
    val featuresIndex = dataset.schema.indexOf(featuresCol)
    val predict = {
      (row: Row) =>
        model.predict(row.getAs[Vector](featuresIndex))
    }

    dataset.withFeature(StructField(predictionCol, DoubleType), predict)
  }
}
