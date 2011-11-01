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
package com.reportgrid.quirrel

import edu.uwm.cs.gll.ast.Node
import scala.annotation.tailrec
import scala.collection.parallel.ParSet

trait Solver extends parser.AST {
  import Function._
  
  // VERY IMPORTANT!!!  each rule must represent a monotonic reduction in tree complexity
  private val Rules: ParSet[PartialFunction[Expr, Set[Expr]]] = ParSet()
  
  def solve(tree: Expr)(predicate: PartialFunction[Node, Boolean]): Expr => Option[Expr] = tree match {
    case n if predicate.isDefinedAt(n) && predicate(n) => Some apply _
    case Add(loc, left, right) => solveBinary(tree, left, right, predicate)(sub(loc), sub(loc))
    case Sub(loc, left, right) => solveBinary(tree, left, right, predicate)(add(loc), sub(loc) andThen { _ andThen neg(loc) })
    case Mul(loc, left, right) => solveBinary(tree, left, right, predicate)(div(loc), div(loc))
    case Div(loc, left, right) => solveBinary(tree, left, right, predicate)(mul(loc), flip(div(loc)))
    case Neg(loc, child) => solve(child)(predicate) andThen { _ map neg(loc) }
    case _ => const(None) _
  }
  
  private def solveBinary(tree: Expr, left: Expr, right: Expr, predicate: PartialFunction[Node, Boolean])(invertLeft: Expr => Expr => Expr, invertRight: Expr => Expr => Expr): Expr => Option[Expr] = {
    val totalPred = predicate.lift andThen { _ getOrElse false }
    val inLeft = isSubtree(totalPred)(left)
    val inRight = isSubtree(totalPred)(right)
    
    if (inLeft && inRight) {
      val results = simplify(tree, predicate) map { solve(_)(predicate) }
      
      results.foldLeft(const[Option[Expr], Expr](None) _) { (acc, f) => e =>
        acc(e) orElse f(e)
      }
    } else if (inLeft && !inRight) {
      solve(left)(predicate) andThen { _ map flip(invertLeft)(right) }
    } else if (!inLeft && inRight) {
      solve(right)(predicate) andThen { _ map flip(invertRight)(left) }
    } else {
      const(None)
    }
  }
  
  private def simplify(tree: Expr, predicate: PartialFunction[Node, Boolean]) =
    search(predicate, ParSet(tree), ParSet(), ParSet()).seq
  
  @tailrec
  private[this] def search(predicate: PartialFunction[Node, Boolean], work: ParSet[Expr], seen: ParSet[Expr], results: ParSet[Expr]): ParSet[Expr] = {
    val filteredWork = work &~ seen
    if (filteredWork.isEmpty) {
      results
    } else {
      val (results2, newWork) = filteredWork partition isSimplified(predicate)
      search(predicate, newWork flatMap possibilities, seen ++ filteredWork, results ++ results2)
    }
  }
  
  private def isSimplified(predicate: PartialFunction[Node, Boolean])(tree: Expr) = false
  
  private def possibilities(expr: Expr): ParSet[Expr] =
    Rules filter { _ isDefinedAt expr } flatMap { _(expr) }
  
  def isSubtree(predicate: Node => Boolean)(tree: Node): Boolean =
    predicate(tree) || (tree.children map isSubtree(predicate) exists identity)
  
  private val add = curried(Add)
  private val sub = curried(Sub)
  private val mul = curried(Mul)
  private val div = curried(Div)
  private val neg = curried(Neg)
  
  private def flip[A, B, C](f: A => B => C)(b: B)(a: A) = f(a)(b)
}
