package com.precog
package daze

import scalaz.Monoid

import com.precog.util._
import scalaz.std.map._

trait ReductionFinder extends DAG {
  import instructions._

  def findReductions(node: DepGraph): Map[DepGraph, Vector[Reduction]] = node.foldDown[Map[DepGraph, Vector[Reduction]]] {
    case node @ dag.Reduce(loc, red, parent) => Map(parent -> Vector(red))
  }
}
