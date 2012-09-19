package com.precog
package daze

import scalaz.Monoid

import scala.collection.mutable

import com.precog.util._

import scalaz.NonEmptyList
import scalaz.std.map._

trait ReductionFinder extends DAG {
  import instructions._
  import dag._

  def findReductions(node: DepGraph): Map[DepGraph, NonEmptyList[Reduction]] = node.foldDown[Map[DepGraph, NonEmptyList[Reduction]]] {
    case dag.Reduce(_, red, parent) => Map(parent -> NonEmptyList(red))
  }

  def megaReduce(node: DepGraph, reds: Map[DepGraph, NonEmptyList[Reduction]]): DepGraph = {
    val reduceTable = mutable.Map[DepGraph, dag.MegaReduce]()  //this is a Map from parent node to MegaReduce node

    node.mapDown { recurse => {
      case graph @ dag.Reduce(loc, red, parent) if reds isDefinedAt parent => {
        val left = reduceTable.get(parent) getOrElse {
          val result = dag.MegaReduce(loc, reds(parent), recurse(parent))
          reduceTable += (parent -> result)
          result
        }
        val index: Int = reds(parent).list.indexOf(red)
        dag.Join(loc, DerefArray, CrossLeftSort, left, Root(loc, PushNum(index.toString)))
      }
    }}
  }
}
