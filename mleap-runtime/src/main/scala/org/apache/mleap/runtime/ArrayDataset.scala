package org.apache.mleap.runtime

/**
  * Created by hwilkins on 11/2/15.
  */
object ArrayDataset {
  val empty: ArrayDataset = ArrayDataset(Array())
}

case class ArrayDataset(data: Array[Row]) extends Dataset {
   override def map(f: (Row) => Row): ArrayDataset = {
     val data2 = data.map(f)
     copy(data = data2)
   }
 }
