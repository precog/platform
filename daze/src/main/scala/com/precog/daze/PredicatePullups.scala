package com.precog
package daze

import scala.collection.mutable
import com.precog.yggdrasil._
import com.precog.yggdrasil.execution.EvaluationContext

import scalaz._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.monoid._

trait PredicatePullupsModule[M[+_]] extends TransSpecableModule[M] {
  trait PredicatePullups extends TransSpecable {
    import dag._
    import instructions.And

    case class GroupEdit(group: BucketSpec, graphEdit: (DepGraph, DepGraph), specEdit: (dag.BucketSpec, dag.BucketSpec))
    
    def predicatePullups(graph: DepGraph, ctx: EvaluationContext): DepGraph = {
      val edits =
        graph.foldDown(true) {
          case s @ Split(g @ Group(id, target, gchild), schild, _) =>
            
            def extractFilter(spec: dag.BucketSpec): Option[(List[DepGraph], List[dag.BucketSpec])] = spec match {
              case dag.IntersectBucketSpec(left, right) =>
                for {
                  fl <- extractFilter(left)
                  fr <- extractFilter(right)
                } yield fl |+| fr
              case dag.Extra(target) => Some((List(target), Nil))
              case u @ dag.UnfixedSolution(id, expr) if isTransSpecable(expr, target) => Some((Nil, List(u)))
              case other => None
            }
            
            extractFilter(gchild) match {
              case Some((booleans @ (_ :: _), newChildren @ (_ :: _))) => {
                val newChild  = newChildren.reduceLeft(dag.IntersectBucketSpec(_, _))
                val boolean   = booleans.reduceLeft(Join(And, IdentitySort, _, _)(s.loc))
                val newTarget = Filter(IdentitySort, target, boolean)(target.loc)
                List(GroupEdit(g, target -> newTarget, gchild -> newChild))
              }
              case _ => Nil
            }
            
          case other => Nil
        }
      
      edits.foldLeft(graph) {
        case (graph, ge @ GroupEdit(group0, graphEdit, specEdit)) =>
          val (n0, group1) = graph.substituteDown(group0, specEdit)
          val (n1, _) = n0.substituteDown(group1, graphEdit)
          n1
      }
    }
  }
}
