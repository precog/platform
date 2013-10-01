/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog
package mimir

import scalaz.Monoid

import scala.collection.mutable

import com.precog.common._

import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.execution.EvaluationContext

import scalaz.std.map._

trait ReductionFinderModule[M[+_]] extends DAG with EvaluatorMethodsModule[M] with TransSpecableModule[M] {
  type TS1 = trans.TransSpec1
  import library._
  import trans._
  import TableModule.CrossOrder
  import CrossOrder._

  trait ReductionFinder extends EvaluatorMethods with TransSpecable {
    import dag._ 
    import instructions._

    case class ReduceInfo(reduce: dag.Reduce, spec: TransSpec1, ancestor: DepGraph)

    def buildReduceInfo(reduce: dag.Reduce, ctx: EvaluationContext): ReduceInfo = {
      val (spec, ancestor) = findTransSpecAndAncestor(reduce.parent, ctx).getOrElse((Leaf(Source), reduce.parent))
      ReduceInfo(reduce, spec, ancestor)
    }

    def findReductions(node: DepGraph, ctx: EvaluationContext): MegaReduceState = {
      implicit val m = new Monoid[List[dag.Reduce]] {
        def zero: List[dag.Reduce] = Nil
        def append(x: List[dag.Reduce], y: => List[dag.Reduce]) = x ::: y
      }

      val reduces = node.foldDown[List[dag.Reduce]](true) {
        case r: dag.Reduce => List(r)
      } distinct

      val info: List[ReduceInfo] = reduces map { buildReduceInfo(_: dag.Reduce, ctx) }

      // for each reduce node, associate it with its ancestor
      val (ancestorByReduce, specByParent) = info.foldLeft((Map[dag.Reduce, DepGraph](), Map[DepGraph, TransSpec1]())) {
        case ((ancestorByReduce, specByParent), ReduceInfo(reduce, spec, ancestor)) =>
          (ancestorByReduce + (reduce -> ancestor), specByParent + (reduce.parent -> spec))
      }

      // for each ancestor, assemble a list of the parents it created
      val parentsByAncestor = (info groupBy { _.ancestor }).foldLeft(Map[DepGraph, List[DepGraph]]()) {
        case (parentsByAncestor, (ancestor, lst)) =>
          parentsByAncestor + (ancestor -> (lst map { _.reduce.parent } distinct))
      }

      // for each parent, assemble a list of the reduces it created
      val reducesByParent = (info groupBy { _.reduce.parent }).foldLeft(Map[DepGraph, List[dag.Reduce]]()) {
        case (reducesByParent, (parent, lst)) =>
          reducesByParent + (parent -> (lst map { _.reduce }))
      }

      MegaReduceState(ancestorByReduce, parentsByAncestor, reducesByParent, specByParent)
    }

    case class MegaReduceState(
        ancestorByReduce: Map[dag.Reduce, DepGraph],
        parentsByAncestor: Map[DepGraph, List[DepGraph]],
        reducesByParent: Map[DepGraph, List[dag.Reduce]],
        specByParent: Map[DepGraph, TransSpec1]) {
          
      def buildMembers(ancestor: DepGraph): List[(TransSpec1, List[Reduction])] = {
        parentsByAncestor(ancestor) map {
          p => (specByParent(p), reducesByParent(p) map { _.red })
        }
      }
    }

    def megaReduce(node: DepGraph, st: MegaReduceState): DepGraph = {
      val reduceTable = mutable.Map[DepGraph, dag.MegaReduce]() 

      node mapDown { recurse => {
        case graph @ dag.Reduce(red, parent) if st.ancestorByReduce contains graph => {
          val ancestor = st.ancestorByReduce(graph)
          val members = st.buildMembers(ancestor)

          val left = reduceTable get ancestor getOrElse {
            val result = dag.MegaReduce(members, recurse(ancestor))
            reduceTable(ancestor) = result
            result
          }

          val firstIndex = st.parentsByAncestor(ancestor).reverse indexOf parent
          val secondIndex = st.reducesByParent(parent).reverse indexOf graph

          dag.Join(DerefArray, Cross(Some(CrossLeft)), 
            dag.Join(DerefArray, Cross(Some(CrossLeft)), 
              left,
              Const(CLong(firstIndex))(graph.loc))(graph.loc),
            Const(CLong(secondIndex))(graph.loc))(graph.loc)
        }
      }}
    }
  }
}
