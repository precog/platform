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

  def findReductions(node: DepGraph): Map[DepGraph, NonEmptyList[dag.Reduce]] = node.foldDown[Map[DepGraph, NonEmptyList[dag.Reduce]]] {
    case node @ dag.Reduce(_, _, parent) => Map(parent -> NonEmptyList(node))
  }

  def megaReduce(node: DepGraph, reds: Map[DepGraph, NonEmptyList[dag.Reduce]]): DepGraph = {
    val reduceTable = mutable.Map[DepGraph, dag.MegaReduce]()  //map from parent node to MegaReduce node

    node.mapDown { recurse => {
      case graph @ dag.Reduce(loc, red, parent) if reds isDefinedAt parent => {  //TODO do something other than isDefinedAt here? like get followed by getOrElse? 
        val left = reduceTable.get(parent) getOrElse {
          val result = dag.MegaReduce(loc, reds(parent), recurse(parent))
          reduceTable += (parent -> result)
          result
        }
        val index: Int = reds(parent).list.indexOf(graph)
        dag.Join(loc, DerefArray, CrossLeftSort, left, Root(loc, PushNum(index.toString)))
      }
    }}
  }
}
