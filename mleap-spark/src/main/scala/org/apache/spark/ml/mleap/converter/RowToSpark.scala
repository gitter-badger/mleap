package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core.linalg.{Vector => MleapVector}
import org.apache.mleap.runtime.{Row => MleapRow}
import org.apache.spark.sql.Row
import MleapSparkSupport._

/**
  * Created by hwilkins on 11/18/15.
  */
case class RowToSpark(row: MleapRow) {
  def toSpark: Row = {
    val values = row.toArray.map {
      case value: MleapVector => value.toSpark
      case value => value
    }

    Row(values: _*)
  }
}
