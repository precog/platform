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
package daze

import scalaz.Monoid

import scala.collection.mutable

import com.precog.common.json._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.CLong

import scalaz.std.map._

trait TransSpecFinder[M[+_]] extends DAG with EvaluatorMethods[M] with InfixLib[M]  {
  import dag._ 
  import trans._
  import instructions._

  case class ReduceInfo(reduce: dag.Reduce, spec: TransSpec1, ancestor: DepGraph)

  // for a reduce, build the single transpecable chain, ignoring other irrelevant branches
  def buildReduceInfo(reduce: dag.Reduce): ReduceInfo = {
    def loop(graph: DepGraph, f: TransSpec1 => TransSpec1): (TransSpec1, DepGraph) = graph match {
      case Join(_, Eq, _, left, Root(_, value)) =>
        loop(left, t => f(trans.EqualLiteral(t, value, false)))

      case Join(_, Eq, _, Root(_, value), right) =>
        loop(right, t => f(trans.EqualLiteral(t, value, false)))

      case Join(_, NotEq, _, left, Root(_, value)) =>
        loop(left, t => f(trans.EqualLiteral(t, value, true)))

      case Join(_, NotEq, _, Root(_, value), right) =>
        loop(right, t => f(trans.EqualLiteral(t, value, true)))

      case Join(_, instructions.WrapObject, _, Root(_, value), right) =>
        value match {
          case value @ CString(str) => loop(right, t => f(trans.WrapObject(t, str)))
          case _ => (f(Leaf(Source)), graph)
        }

      case Join(_, instructions.DerefObject, _, left, Root(_, value)) =>
        value match {
          case value @ CString(str) => loop(left, t => f(DerefObjectStatic(t, CPathField(str))))
          case _ => (f(Leaf(Source)), graph)
        }
      
      case Join(_, instructions.DerefMetadata, _, left, Root(_, value)) =>
        value match {
          case value @ CString(str) => loop(left, t => f(DerefMetadataStatic(t, CPathMeta(str))))
          case _ => (f(Leaf(Source)), graph)
        }

      case Join(_, DerefArray, _, left, Root(_, value)) =>
        value match {
          case CNum(n) => loop(left, t => f(DerefArrayStatic(t, CPathIndex(n.toInt))))
          case CLong(n) => loop(left, t => f(DerefArrayStatic(t, CPathIndex(n.toInt))))
          case CDouble(n) => loop(left, t => f(DerefArrayStatic(t, CPathIndex(n.toInt))))
          case _ => (f(Leaf(Source)), graph)
        }
      
      case Join(_, instructions.ArraySwap, _, left, Root(_, value)) =>
        value match {
          case CNum(n) => loop(left, t => f(trans.ArraySwap(t, n.toInt)))
          case CLong(n) => loop(left, t => f(trans.ArraySwap(t, n.toInt)))
          case CDouble(n) => loop(left, t => f(trans.ArraySwap(t, n.toInt)))
          case _ => (f(Leaf(Source)), graph)
        }

      case Join(_, instructions.JoinObject, _, left, Root(_, value)) =>
        value match {
          case CEmptyObject => loop(left, t => f(trans.InnerObjectConcat(t)))
          case _ => (f(Leaf(Source)), graph)
        }
                  
      case Join(_, instructions.JoinObject, _, Root(_, value), right) =>
        value match {
          case CEmptyObject => loop(right, t => f(trans.InnerObjectConcat(t)))
          case _ => (f(Leaf(Source)), graph)
        }

      case Join(_, instructions.JoinArray, _, left, Root(_, value)) =>
        value match {
          case CEmptyArray => loop(left, t => f(trans.ArrayConcat(t)))
          case _ => (f(Leaf(Source)), graph)
        }

      case Join(_, instructions.JoinArray, _, Root(_, value), right) =>
        value match {
          case CEmptyArray => loop(right, t => f(trans.ArrayConcat(t)))
          case _ => (f(Leaf(Source)), graph)
        }

      case Join(_, op, _, left, Root(_, value)) =>
        op2ForBinOp(op) map { _.f2.partialRight(value) } match {
          case Some(f1) => loop(left, t => f(trans.Map1(t, f1)))
          case None => (f(Leaf(Source)), graph)
        }
          
      case Join(_, op, CrossLeftSort | CrossRightSort, Root(_, value), right) =>
        op2ForBinOp(op) map { _.f2.partialLeft(value) } match {
          case Some(f1) => loop(right, t => f(trans.Map1(t, f1)))
          case None => (f(Leaf(Source)), graph)
        }

      case dag.Join(_, op, joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => 
        val (targetTrans, targetAncestor) = loop(target, identity _)
        val (booleanTrans, booleanAncestor) = loop(boolean, identity _)

        if (targetAncestor == booleanAncestor) (f(transFromBinOp(op)(targetTrans, booleanTrans)), targetAncestor)
        else (f(Leaf(Source)), graph)

      case dag.Filter(_, joinSort @ (IdentitySort | ValueSort(_)), target, boolean) => 
        val (targetTrans, targetAncestor) = loop(target, identity _)
        val (booleanTrans, booleanAncestor) = loop(boolean, identity _)

        if (targetAncestor == booleanAncestor) (f(trans.Filter(targetTrans, booleanTrans)), targetAncestor)
        else (f(Leaf(Source)), graph)

      case dag.Operate(_, instructions.WrapArray, parent) => loop(parent, t => f(trans.WrapArray(t)))

      case dag.Operate(_, op, parent) => loop(parent, t => f(trans.Map1(t, op1(op).f1)))

      case _ => (f(Leaf(Source)), graph)
    }

    val (spec, ancestor) = loop(reduce.parent, identity _)
    ReduceInfo(reduce, spec, ancestor)
  }
}

trait ReductionFinder[M[+_]] extends TransSpecModule with TransSpecFinder[M] {
  import trans._
  import dag._
  import instructions._

  def findReductions(node: DepGraph): MegaReduceState = {

    implicit val m = new Monoid[List[dag.Reduce]] {
      def zero: List[dag.Reduce] = Nil
      def append(x: List[dag.Reduce], y: => List[dag.Reduce]) = x ++ y
    }

    val reduces = node.foldDown[List[dag.Reduce]] {
      case (r: dag.Reduce) => List(r)
    }.distinct

    val info: List[ReduceInfo] = reduces.map(buildReduceInfo)

    val ancestorByReduce = mutable.Map.empty[dag.Reduce, DepGraph]
    val parentsByAncestor = mutable.Map.empty[DepGraph, List[DepGraph]]
    val reducesByParent = mutable.Map.empty[DepGraph, List[dag.Reduce]]
    val specByParent = mutable.Map.empty[DepGraph, TransSpec1]

    // for each reduce node, associate it with its ancestor
    info.foreach {
      case ReduceInfo(reduce, spec, ancestor) =>
        ancestorByReduce(reduce) = ancestor
        specByParent(reduce.parent) = spec
    }

    // for each ancestor, assemble a list of the parents it created
    info.groupBy(_.ancestor).foreach {
      case (ancestor, lst) => parentsByAncestor(ancestor) = lst.map(_.reduce.parent).distinct
    }

    // for each parent, assemble a list of the reduces it created
    info.groupBy(_.reduce.parent).foreach {
      case (parent, lst) => reducesByParent(parent) = lst.map(_.reduce)
    }

    MegaReduceState(ancestorByReduce, parentsByAncestor, reducesByParent, specByParent)
  }

  case class MegaReduceState(
    ancestorByReduce: mutable.Map[dag.Reduce, DepGraph],
    parentsByAncestor: mutable.Map[DepGraph, List[DepGraph]],
    reducesByParent: mutable.Map[DepGraph, List[dag.Reduce]],
    specByParent: mutable.Map[DepGraph, TransSpec1]
  ) {
    def buildMembers(ancestor: DepGraph): List[(TransSpec1, List[Reduction])] = {
      parentsByAncestor(ancestor).map {
        p => (specByParent(p), reducesByParent(p).map(_.red))
      }
    }
  }

  def megaReduce(node: DepGraph, st: MegaReduceState): DepGraph = {

    val reduceTable = mutable.Map[DepGraph, dag.MegaReduce]()  

    node.mapDown { recurse => {
      case graph @ dag.Reduce(loc, red, parent) if st.ancestorByReduce.contains(graph) => {
        val ancestor = st.ancestorByReduce(graph)
        val members = st.buildMembers(ancestor)

        val left = reduceTable.get(ancestor) getOrElse {
          val result = dag.MegaReduce(loc, members, recurse(ancestor))
          reduceTable(ancestor) = result
          result
        }

        val firstIndex = st.parentsByAncestor(ancestor).reverse.indexOf(parent)
        val secondIndex = st.reducesByParent(parent).reverse.indexOf(graph)

        dag.Join(loc, DerefArray, CrossLeftSort, 
          dag.Join(loc, DerefArray, CrossLeftSort, 
          left, 
          Root(loc, CLong(firstIndex))),
        Root(loc, CLong(secondIndex)))
      }
    }}
  }
}
