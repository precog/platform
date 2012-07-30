package com.precog
package daze

import scalaz.Monoid

import com.precog.util._
import scalaz.std.map._

trait ReductionFinder extends DAG {
  import instructions._
  import dag._

  def findReductions(node: DepGraph): Map[DepGraph, Vector[dag.Reduce]] = node.foldDown[Map[DepGraph, Vector[dag.Reduce]]] {
    case node @ dag.Reduce(_, _, parent) => Map(parent -> Vector(node))
  }

  def megaReduce(node: DepGraph, reds: Map[DepGraph, Vector[dag.Reduce]]): DepGraph = node.mapDown { recurse => {
    case graph if reds isDefinedAt graph => {
      dag.MegaReduce(graph.loc, reds(graph), recurse(graph))
    }
    case r @ dag.Reduce(loc, red, parent) => {
      val index: Int = reds(parent).indexOf(r)
      dag.Join(loc, DerefArray, CrossLeftSort, dag.MegaReduce(loc, reds(parent), recurse(parent)), Root(loc, PushNum("`index`")))
    }
  }}
}
