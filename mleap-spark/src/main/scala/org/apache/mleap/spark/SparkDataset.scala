package org.apache.mleap.spark

import org.apache.mleap.runtime.{Row, Dataset}
import org.apache.spark.rdd.RDD

/**
 * Created by hwilkins on 11/6/15.
 */
case class SparkDataset(rdd: RDD[Row]) extends Dataset {
  override def map(f: (Row) => Row): SparkDataset = copy(rdd = rdd.map(f))
}
