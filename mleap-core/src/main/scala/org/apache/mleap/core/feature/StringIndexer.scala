package org.apache.mleap.core.feature

/**
 * Created by hwilkins on 11/5/15.
 */
case class StringIndexer(strings: Array[String]) extends Serializable {
  def stringToIndex: Map[String, Int] = strings.zipWithIndex.toMap

  def apply(value: String): Double = stringToIndex(value)
}
