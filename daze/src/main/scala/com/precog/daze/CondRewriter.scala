package com.precog
package daze

trait CondRewriter extends DAG {
  import instructions._
  import dag._

  def rewriteConditionals(node: DepGraph, splits: Set[dag.Split]): DepGraph = {
    node.mapDown({ recurse => {
      case peer @ IUI(true, Filter(leftJoin, left, pred1), Filter(rightJoin, right, Operate(Comp, pred2))) if pred1 == pred2 => 
        Cond(recurse(pred1), recurse(left), leftJoin, recurse(right), rightJoin)(peer.loc)
    }}, splits)
  }
}
