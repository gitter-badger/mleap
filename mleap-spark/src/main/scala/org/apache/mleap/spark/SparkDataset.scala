package org.apache.mleap.spark

import org.apache.mleap.runtime.{Row, Dataset}
import org.apache.spark.rdd.RDD

/**
 * Created by hwilkins on 11/6/15.
 */
case class SparkDataset(rdd: RDD[(Row, Seq[Any])]) extends Dataset {
  override def map(f: (Row) => Row): SparkDataset = {
    val rdd2 = rdd.map {
      case (row, sparkRow) => (f(row), sparkRow)
    }
    copy(rdd = rdd2)
  }

  override def toArray: Array[Row] = rdd.map(_._1).collect
}
