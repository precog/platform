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
package com.precog.quirrel

import com.codecommit.gll.ast.Node
import com.codecommit.gll.LineStream

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.parallel.ParSet

trait Solver extends parser.AST with typer.Binder {
  import Function._
  import ast._
  
  private[this] val enableTrace = false
  
  def solve(tree: Expr, sigma: Sigma)(partialPred: PartialFunction[Node, Boolean]): Expr => Option[Expr] = {
    val predicate = partialPred.lift andThen { _ getOrElse false }
    
    def sigmaFormal(d: Dispatch): Option[Expr] = d.binding match {
      case FormalBinding(let) => sigma get ((d.name, let))
      case _ => None
    }
    
    // VERY IMPORTANT!!!  each rule must represent a monotonic reduction in tree complexity
    lazy val Rules: Set[PartialFunction[Expr, Set[Expr]]] = Set(
      { case Add(loc, left, right) if left equalsIgnoreLoc right => Set(Mul(loc, NumLit(loc, "2"), left)) },
      { case Sub(loc, left, right) if left equalsIgnoreLoc right => Set(NumLit(loc, "0")) },
      { case Div(loc, left, right) if left equalsIgnoreLoc right => Set(NumLit(loc, "1")) },
      
      { case Add(loc, left, right) => Set(Add(loc, right, left)) },
      { case Sub(loc, left, right) => Set(Add(loc, Neg(loc, right), left)) },
      { case Add(loc, Neg(_, left), right) => Set(Sub(loc, right, left)) },
      { case Mul(loc, left, right) => Set(Mul(loc, right, left)) },
      
      { case Add(loc, Add(loc2, x, y), z) => Set(Add(loc, x, Add(loc2, y, z))) },
      { case Mul(loc, Mul(loc2, x, y), z) => Set(Mul(loc, x, Mul(loc2, y, z))) },
      
      { case Add(loc, Mul(loc2, x, y), z) if y equalsIgnoreLoc z => Set(Mul(loc2, Add(loc, x, NumLit(loc, "1")), y)) },
      { case Add(loc, Mul(loc2, w, x), Mul(loc3, y, z)) if x equalsIgnoreLoc z => Set(Mul(loc2, Add(loc, w, y), x)) },
      
      { case Sub(loc, Mul(loc2, x, y), z) if y equalsIgnoreLoc z => Set(Mul(loc2, Sub(loc, x, NumLit(loc, "1")), y)) },
      { case Sub(loc, Mul(loc2, w, x), Mul(loc3, y, z)) if x equalsIgnoreLoc z => Set(Mul(loc2, Sub(loc, w, y), x)) },
      
      { case Add(loc, Div(loc2, x, y), z) => Set(Div(loc2, Add(loc, x, Mul(loc, z, y)), y)) },
      { case Add(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc2, Add(loc, Mul(loc2, w, z), Mul(loc3, y, x)), Mul(loc2, x, z))) },
      
      { case Mul(loc, Div(loc2, x, y), z) => Set(Div(loc2, Mul(loc, x, z), y)) },
      { case Mul(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc2, Mul(loc, w, y), Mul(loc, x, z))) },
      
      { case Div(_, Mul(_, x, y), z) if x equalsIgnoreLoc z => Set(y) },
      { case Div(loc, Mul(_, w, x), Mul(_, y, z)) if w equalsIgnoreLoc y => Set(Div(loc, x, z)) },
      
      { case Div(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc, Mul(loc2, w, z), Mul(loc3, x, y))) },
      
      { case Sub(loc, Div(loc2, x, y), z) => Set(Div(loc2, Sub(loc, x, Mul(loc, z, y)), y)) },
      { case Sub(loc, Div(loc2, w, x), Div(loc3, y, z)) => Set(Div(loc2, Sub(loc, Mul(loc2, w, z), Mul(loc3, y, x)), Mul(loc2, x, z))) },
      
      { case Neg(loc, Add(loc2, x, y)) => Set(Add(loc2, Neg(loc, x), Neg(loc, y))) },
      { case Neg(loc, Sub(loc2, x, y)) => Set(Sub(loc2, Neg(loc, x), Neg(loc, y))) },
      { case Neg(loc, Mul(loc2, x, y)) => Set(Mul(loc2, Neg(loc, x), y), Mul(loc2, x, Neg(loc, y))) },
      { case Neg(loc, Div(loc2, x, y)) => Set(Div(loc2, Neg(loc, x), y), Div(loc2, x, Neg(loc, y))) },
      { case Neg(_, Neg(_, x)) => Set(x) },
      
      { case Add(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Add(loc, left2, right2) },
      { case Sub(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Sub(loc, left2, right2) },
      { case Mul(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Mul(loc, left2, right2) },
      { case Div(loc, left, right) => for (left2 <- possibilities(left) + left; right2 <- possibilities(right) + right) yield Div(loc, left2, right2) },
      
      { case Neg(loc, child) => possibilities(child) map neg(loc) },
      { case Paren(_, child) => Set(child) },
      
      { case d @ Dispatch(_, id, _) if sigmaFormal(d).isDefined => sigmaFormal(d).toSet })
    
    def inner(tree: Expr): Expr => Option[Expr] = tree match {
      case n if predicate(n) => Some apply _
      
      case tree @ Dispatch(_, id, actuals) => {
        tree.binding match {
          case LetBinding(let) => {
            val ids = let.params map { Identifier(Vector(), _) }
            val sigma2 = sigma ++ (ids zip Stream.continually(let) zip actuals)
            solve(let.left, sigma2)(partialPred)      // not inner!
          }
          
          case FormalBinding(let) => {
            val actualM = sigma get ((id, let))
            val resultM = actualM map { solve(_, sigma)(partialPred) }
            resultM getOrElse sys.error("er...?")
          }
          
          case _ => const(None) _
        }
      }
      
      case Add(loc, left, right) => solveBinary(tree, left, right)(sub(loc), sub(loc))
      case Sub(loc, left, right) => solveBinary(tree, left, right)(add(loc), sub(loc) andThen { _ andThen neg(loc) })
      case Mul(loc, left, right) => solveBinary(tree, left, right)(div(loc), div(loc))
      case Div(loc, left, right) => solveBinary(tree, left, right)(mul(loc), flip(div(loc)))
      case Neg(loc, child) => inner(child) andThen { _ map neg(loc) }
      case Paren(_, child) => inner(child)
      case _ => const(None) _
    }
    
    def solveBinary(tree: Expr, left: Expr, right: Expr)(invertLeft: Expr => Expr => Expr, invertRight: Expr => Expr => Expr): Expr => Option[Expr] = {
      val inLeft = isSubtree(left)
      val inRight = isSubtree(right)
      
      if (inLeft && inRight) {
        val results = simplify(tree) map { xs =>
          (inner(xs.head), xs)
        }
        
        results.foldLeft(const[Option[Expr], Expr](None) _) { 
          case (acc, (f, trace)) => e =>
            acc(e) orElse (f(e) map { e2 => printTrace(trace); e2 })
        }
      } else if (inLeft && !inRight) {
        inner(left) compose flip(invertLeft)(right)
      } else if (!inLeft && inRight) {
        inner(right) compose flip(invertRight)(left)
      } else {
        const(None)
      }
    }
    
    def simplify(tree: Expr) =
      search(Set(tree :: Nil), Set(), Set())
    
    @tailrec
    def search(work: Set[List[Expr]], seen: Set[Expr], results: Set[List[Expr]]): Set[List[Expr]] = {
      val filteredWork = work filterNot { xs => seen(xs.head) }
      // println("Examining: " + (filteredWork map { _.head } map printInfix))
      if (filteredWork.isEmpty) {
        results
      } else {
        val (results2, newWork) = filteredWork partition { xs => isSimplified(xs.head) }
        val newWorkLists = newWork flatMap { xs =>
          possibilities(xs.head) map { _ :: xs }
        }
        
        // return just the first set of results we find
        if (results2.isEmpty)
          search(newWorkLists, seen ++ (filteredWork map { _.head }), results ++ results2)
        else
          results2
      }
    }
    
    def isSimplified(tree: Expr) = tree match {
      case Add(_, left, right) => !isSubtree(left) || !isSubtree(right)
      case Sub(_, left, right) => !isSubtree(left) || !isSubtree(right)
      case Mul(_, left, right) => !isSubtree(left) || !isSubtree(right)
      case Div(_, left, right) => !isSubtree(left) || !isSubtree(right)
      case _ => !isSubtree(tree)
    }
    
    def possibilities(expr: Expr): Set[Expr] =
      Rules filter { _ isDefinedAt expr } flatMap { _(expr) }
    
    def isSubtree(tree: Node): Boolean = {
      def inner(tree: Node, sigma: Sigma): Boolean = tree match {
        case tree if predicate(tree) => true
        
        case tree @ Dispatch(_, id, actuals) => {
          tree.binding match {
            case LetBinding(let) => {
              val ids = let.params map { Identifier(Vector(), _) }
              val sigma2 = sigma ++ (ids zip Stream.continually(let) zip actuals)
              inner(let.left, sigma2)
            }
            
            case FormalBinding(let) => {
              val actualM = sigma get ((id, let))
              val resultM = actualM map { inner(_, sigma) }
              resultM getOrElse sys.error("er...?")
            }
            
            case _ => false
          }
        }
        
        case _ => tree.children map { inner(_, sigma) } exists identity
      }
      
      inner(tree, sigma)
    }
    
    inner(tree)
  }

  /**
   * Note that this really only works for `Eq` at present.  Will improve things
   * further in future.
   */
  def solveRelation(re: ComparisonOp, sigma: Sigma)(predicate: PartialFunction[Node, Boolean]): Option[Expr] = {
    val leftRight: Option[(LineStream, Expr, Expr)] = re match {
      case Lt(_, _, _) => None
      case LtEq(_, _, _) => None
      case Gt(_, _, _) => None
      case GtEq(_, _, _) => None
      
      case Eq(loc, left, right) => Some((loc, left, right))
      case NotEq(_, _, _) => None
    }
    
    val result = leftRight flatMap {
      case (loc, left, right) if predicate.isDefinedAt(left) && predicate(left) =>
        Some(right)
      
      case (loc, left, right) if predicate.isDefinedAt(right) && predicate(right) =>
        Some(left)
      
      case (loc, left, right) => {
        // try both addition and multiplication groups
        lazy val sub = Sub(loc, left, right)
        lazy val div = Div(loc, left, right)
        
        lazy val first = solve(sub, sigma)(predicate)(NumLit(loc, "0"))
        lazy val second = solve(div, sigma)(predicate)(NumLit(loc, "1"))
        
        first orElse second
      }
    }
    
    // big assumption here!!!!  we're assuming that these phases only require locally-synthetic attributes
    result foreach { e => bindRoot(e, e) }
    
    result
  }
  
  def solveComplement(c: Comp, sigma: Sigma): PartialFunction[Node, Boolean] => Option[Expr] = c.child match {
    case Lt(loc, left, right) => solveRelation(GtEq(loc, left, right), sigma)
    case LtEq(loc, left, right) => solveRelation(Gt(loc, left, right), sigma)
    case Gt(loc, left, right) => solveRelation(LtEq(loc, left, right), sigma)
    case GtEq(loc, left, right) => solveRelation(Lt(loc, left, right), sigma)
    
    case Eq(loc, left, right) => solveRelation(NotEq(loc, left, right), sigma)
    case NotEq(loc, left, right) => solveRelation(Eq(loc, left, right), sigma)
    
    case _ => const(None)
  }
  
  private def printTrace(trace: List[Expr]) {
    if (enableTrace) {
      println("*** Solution Point!")
      println(trace.reverse map { e => printSExp(e) } mkString "\n\n")
    }
  }
  
  private val add = Add.curried
  private val sub = Sub.curried
  private val mul = Mul.curried
  private val div = Div.curried
  private val neg = Neg.curried
  
  private def flip[A, B, C](f: A => B => C)(b: B)(a: A) = f(a)(b)
}
