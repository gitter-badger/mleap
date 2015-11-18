package org.apache.spark.ml.mleap.converter

import org.apache.mleap.core
import org.apache.spark.ml.tree.{LeafNode, InternalNode, Node}
import MleapSparkSupport._

/**
  * Created by hwilkins on 11/18/15.
  */
case class NodeToMleap(node: Node) {
  def toMleap: core.tree.Node = {
    node match {
      case node: InternalNode =>
        core.tree.InternalNode(node.prediction,
          node.impurity,
          node.gain,
          node.leftChild.toMleap,
          node.rightChild.toMleap,
          node.split.toMleap)
      case node: LeafNode =>
        core.tree.LeafNode(node.prediction, node.impurity)
    }
  }
}