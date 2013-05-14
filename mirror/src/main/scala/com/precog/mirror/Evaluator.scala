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
package mirror

import quirrel._
import quirrel.typer._
import quirrel.emitter._
import util.IdGen

import blueeyes.json._

import scalaz.Ordering
import scalaz.syntax.semigroup._

import scala.collection.mutable

trait EvaluatorModule extends ProvenanceChecker
    with Binder
    with GroupSolver
    with Compiler
    with LineErrors
    with LibraryModule {
      
  import ast._
  
  private type Identities = Vector[Int]
  private type Dataset = Seq[(Identities, JValue)]
  
  // TODO more specific sequence types
  def eval(expr: Expr)(fs: String => Seq[JValue]): Seq[JValue] = {
    val inits = mutable.Map[String, Int]()
    val news = mutable.Map[New, Int]()
    val IdGen = new IdGen
    
    def mappedFS(path: String): Seq[(Identities, JValue)] = {
      val raw = fs(path)
      
      val init = inits get path getOrElse {
        val init = IdGen.nextInt()
        inits += (path -> init)
        
        0 until raw.length foreach { _ => IdGen.nextInt() }     // ew....
        
        init
      }
      
      // done in this order for eagerness reasons
      raw zip (Stream from init map { Vector(_) }) map { case (a, b) => (b, a) }
    }
    
    def loop(env: Map[(Let, String), Dataset])(expr: Expr): Dataset = expr match {
      case Let(loc, _, _, _, right) => loop(env)(right)
      
      case Solve(loc, constraints, child) =>
        sys.error("todo")
      
      case Import(_, _, child) => loop(env)(child)
      
      case Assert(loc, pred, child) => {
        val result = loop(env)(pred) forall {
          case (_, JBool(b)) => b
          case _ => true
        }
        
        if (result)
          loop(env)(child)
        else
          throw new RuntimeException("assertion failed: %d:%d".format(loc.lineNum, loc.colNum))
      }
      
      case Observe(_, _, _) => sys.error("todo")
      
      case expr @ New(_, child) => {
        val raw = loop(env)(child) map { case (_, v) => v }
        
        val init = news get expr getOrElse {
          val init = IdGen.nextInt()
          news += (expr -> init)
          
          0 until raw.length foreach { _ => IdGen.nextInt() }     // ew....
          
          init
        }
        
        // done in this order for eagerness reasons
        raw zip (Stream from init map { Vector(_) }) map { case (a, b) => (b, a) }
      }
      
      // TODO add support for scoped constraints
      case Relate(_, _, _, in) => loop(env)(in)
      
      case TicVar(_, _) => sys.error("todo")
      
      case StrLit(_, value) => (Vector(), JString(value)) :: Nil
      
      case NumLit(_, value) => (Vector(), JNumBigDec(BigDecimal(value))) :: Nil
      
      case BoolLit(_, true) => (Vector(), JTrue) :: Nil
      case BoolLit(_, false) => (Vector(), JFalse) :: Nil
      
      case UndefinedLit(_) => Nil
      
      case NullLit(_) => (Vector(), JNull) :: Nil
      
      case ObjectDef(loc, props) => sys.error("todo")
      case ArrayDef(loc, props) => sys.error("todo")
      
      case Descent(loc, child, property) => {
        loop(env)(child) map {
          case (ids, value) => (ids, value \ property)
        }
      }
      
      case MetaDescent(_, _, _) => sys.error("todo")
      
      case Deref(_, _, _) => sys.error("todo")
      
      case expr @ Dispatch(loc, Identifier(ns, id), actuals) => {
        val actualSets = actuals map loop(env)
        
        expr.binding match {
          case LetBinding(b) => {
            val env2 = env ++ ((Stream continually b) zip b.params zip actualSets)
            loop(env2)(b.left)
          }
          
          case FormalBinding(b) => env((b, id))
          
          case LoadBinding => {
            actualSets.head collect {
              case (_, JString(path)) => mappedFS(path)
            } flatten
          }
          
          case ExpandGlobBinding => actualSets.head       // TODO
          
          case _ => sys.error("todo")
        }
      }
          
      case Cond(_, _, _, _) => sys.error("todo")
      
      case Where(_, _, _) => sys.error("todo")
      
      case With(_, _, _) => sys.error("todo")
      
      case Union(_, _, _) => sys.error("todo")
      case Intersect(_, _, _) => sys.error("todo")
      case Difference(_, _, _) => sys.error("todo")
      
      case Add(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN + rightN)
        }
      }
      
      case Sub(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN - rightN)
        }
      }
      
      case Mul(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN * rightN)
        }
      }
      
      case Div(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN / rightN)
        }
      }
      
      case Mod(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN % rightN)
        }
      }
      
      case Pow(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNumLong(rightN)) => JNum(leftN pow rightN.toInt)
        }
      }
      
      case Lt(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN < rightN)
        }
      }
      
      case LtEq(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN <= rightN)
        }
      }
      
      case Gt(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN > rightN)
        }
      }
      
      case GtEq(_, left, right) => {
        join(env)(left, right) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN >= rightN)
        }
      }
      
      case Eq(_, left, right) => {
        join(env)(left, right) {
          case (leftV, rightV) => JBool(leftV == rightV)
        }
      }
      
      case NotEq(_, left, right) => {
        join(env)(left, right) {
          case (leftV, rightV) => JBool(leftV != rightV)
        }
      }
      
      case And(_, left, right) => {
        join(env)(left, right) {
          case (JBool(leftB), JBool(rightB)) => JBool(leftB && rightB)
        }
      }
      
      case Or(_, left, right) => {
        join(env)(left, right) {
          case (JBool(leftB), JBool(rightB)) => JBool(leftB || rightB)
        }
      }
      
      case Comp(_, child) => {
        loop(env)(child) collect {
          case (ids, JBool(value)) => (ids, JBool(!value))
        }
      }
      
      case Neg(_, child) => {
        loop(env)(child) collect {
          case (ids, JNum(value)) => (ids, JNum(-value))
        }
      }
      
      case Paren(_, child) => loop(env)(child)
    }
    
    def join(env: Map[(Let, String), Dataset])(left: Expr, right: Expr)(pf: PartialFunction[(JValue, JValue), JValue]): Dataset = {
      val leftRes = loop(env)(left)
      val rightRes = loop(env)(right)
      
      // TODO compute join keys
      val indicesLeft = List(0)
      val indicesRight = List(0)
      
      // TODO compute merge key
      val mergeKey: List[Either[Int, Int]] = List(Left(0))
      
      val joined = zipAlign(leftRes, rightRes) {
        case ((idsLeft, _), (idsRight, _)) => {
          val zipped = (indicesLeft map idsLeft) zip (indicesRight map idsRight)
          
          zipped map {
            case (x, y) => Ordering.fromInt(x - y)
          } reduce { _ |+| _ }
        }
      }
      
      joined collect {
        case ((idsLeft, leftV), (idsRight, rightV)) if pf.isDefinedAt((leftV, rightV)) => {
          val idsMerged = mergeKey map {
            case Left(i) => idsLeft(i)
            case Right(i) => idsRight(i)
          }
          
          (Vector(idsMerged: _*), pf(leftV, rightV))
        }
      }
    }
    
    if (expr.errors.isEmpty) {
      loop(Map())(expr) map {
        case (_, value) => value
      }
    } else {
      Seq.empty
    }
  }
  
  /**
   * Poor-man's cogroup specialized on the middle case
   */
  private def zipAlign[A, B](left: Seq[A], right: Seq[B])(f: (A, B) => Ordering): Seq[(A, B)] = {
    if (left.isEmpty || right.isEmpty) {
      Nil
    } else {
      f(left.head, right.head) match {
        case Ordering.EQ => (left.head, right.head) +: zipAlign(left.tail, right.tail)(f)
        case Ordering.LT => zipAlign(left.tail, right)(f)
        case Ordering.GT => zipAlign(left, right.tail)(f)
      }
    }
  }
}
