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

import scalaz.{Order, Ordering}
import scalaz.syntax.semigroup._

import scala.collection.mutable

// TODO soup up all provenance sites with resolution of ParametricProvenance
trait EvaluatorModule extends ProvenanceChecker
    with Binder
    with GroupSolver
    with Compiler
    with LineErrors
    with LibraryModule {
      
  import ast._
  
  private type Identities = Vector[Int]
  private type SEvent = (Identities, JValue)
  private type Dataset = Seq[SEvent]
  
  private implicit val SEOrder: Order[SEvent] = new Order[SEvent] {
    def order(left: SEvent, right: SEvent): Ordering = {
      val (leftIds, leftValue) = left
      val (rightIds, rightValue) = right
      
      val idOrder = leftIds zip rightIds map {
        case (li, ri) => Ordering.fromInt(li - ri)
      } reduceOption { _ |+| _ } getOrElse Ordering.EQ
      
      idOrder |+| JValue.order.order(leftValue, rightValue)
    }
  }
  
  // TODO more specific sequence types
  def eval(expr: Expr)(fs: String => Seq[JValue]): Seq[JValue] = {
    val inits = mutable.Map[String, Int]()
    val news = mutable.Map[New, Int]()
    val IdGen = new IdGen
    
    def mappedFS(path: String): Dataset = {
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
    
    def loop(env: Map[(Let, String), Dataset], restrict: Map[Provenance, Set[Identities]])(expr: Expr): Dataset = expr match {
      case Let(loc, _, _, _, right) => loop(env, restrict)(right)
      
      case Solve(loc, constraints, child) =>
        sys.error("todo")
      
      case Import(_, _, child) => loop(env, restrict)(child)
      
      case Assert(loc, pred, child) => {
        val result = loop(env, restrict)(pred) forall {
          case (_, JBool(b)) => b
          case _ => true
        }
        
        if (result)
          loop(env, restrict)(child)
        else
          throw new RuntimeException("assertion failed: %d:%d".format(loc.lineNum, loc.colNum))
      }
      
      case Observe(_, _, _) => sys.error("todo")
      
      case expr @ New(_, child) => {
        val raw = loop(env, restrict)(child) map { case (_, v) => v }
        
        val init = news get expr getOrElse {
          val init = IdGen.nextInt()
          news += (expr -> init)
          
          0 until raw.length foreach { _ => IdGen.nextInt() }     // ew....
          
          init
        }
        
        // done in this order for eagerness reasons
        raw zip (Stream from init map { Vector(_) }) map { case (a, b) => (b, a) }
      }
      
      case Relate(_, from, to, in) => {
        val fromRes = loop(env, restrict)(from)
        val toRes = loop(env, restrict)(to)
        
        val fromIdx = Set(fromRes map { case (ids, _) => ids }: _*)
        val toIdx = Set(toRes map { case (ids, _) => ids }: _*)
        
        loop(env, restrict + (from.provenance -> fromIdx) + (to.provenance -> toIdx))(in)
      }
      
      case TicVar(_, _) => sys.error("todo")
      
      case StrLit(_, value) => (Vector(), JString(value)) :: Nil
      
      case NumLit(_, value) => (Vector(), JNumBigDec(BigDecimal(value))) :: Nil
      
      case BoolLit(_, true) => (Vector(), JTrue) :: Nil
      case BoolLit(_, false) => (Vector(), JFalse) :: Nil
      
      case UndefinedLit(_) => Nil
      
      case NullLit(_) => (Vector(), JNull) :: Nil
      
      case ObjectDef(loc, props) => {
        val propResults = props map {
          case (name, expr) =>
            (name, loopForJoin(env, restrict)(expr), expr.provenance)
        }
        
        val wrappedResults = propResults map {
          case (name, data, prov) => {
            val mapped = data map {
              case (ids, v) => (ids, JObject(Map(name -> v)): JValue)
            }
            
            (mapped, prov)
          }
        }
        
        val resultOpt = wrappedResults.reduceLeftOption[(Dataset, Provenance)]({
          case ((left, leftProv), (right, rightProv)) => {
            val back = handleBinary(left, leftProv, right, rightProv) {
              case (JObject(leftFields), JObject(rightFields)) =>
                JObject(leftFields ++ rightFields)
            }
            
            // would have failed to type check otherwise
            val prov = unifyProvenance(expr.relations)(leftProv, rightProv).get
            
            (back, prov)
          }
        })
        
        resultOpt map { case (data, _) => data } getOrElse {
          (Vector(), JArray(Nil)) :: Nil
        }
      }
      
      case ArrayDef(loc, values) => {
        val valueResults = values map { expr =>
          (loopForJoin(env, restrict)(expr), expr.provenance)
        }
        
        val wrappedValues = valueResults map {
          case (data, prov) => {
            val mapped = data map {
              case (ids, v) => (ids, JArray(v :: Nil): JValue)
            }
            
            (mapped, prov)
          }
        }
        
        val resultOpt = wrappedValues.reduceLeftOption[(Dataset, Provenance)]({
          case ((left, leftProv), (right, rightProv)) => {
            val back = handleBinary(left, leftProv, right, rightProv) {
              case (JArray(leftValues), JArray(rightValues)) =>
                JArray(leftValues ++ rightValues)
            }
            
            // would have failed to type check otherwise
            val prov = unifyProvenance(expr.relations)(leftProv, rightProv).get
            
            (back, prov)
          }
        })
        
        resultOpt map { case (data, _) => data } getOrElse {
          (Vector(), JArray(Nil)) :: Nil
        }
      }
      
      case Descent(loc, child, property) => {
        loop(env, restrict)(child) collect {
          case (ids, value: JObject) => (ids, value \ property)
        }
      }
      
      case MetaDescent(_, _, _) => sys.error("todo")
      
      case Deref(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JArray(values), JNum(index)) =>
            values(index.toInt)
        }
      }
      
      case expr @ Dispatch(loc, Identifier(ns, id), actuals) => {
        val actualSets = actuals map loopForJoin(env, restrict)
        
        expr.binding match {
          case LetBinding(b) => {
            val env2 = env ++ ((Stream continually b) zip b.params zip actualSets)
            loop(env2, restrict)(b.left)
          }
          
          case FormalBinding(b) => env((b, id))
          
          case LoadBinding => {
            actualSets.head collect {
              case (_, JString(path)) => mappedFS(path)
            } flatten
          }
          
          case ExpandGlobBinding => actualSets.head       // TODO
          
          case Op1Binding(op1) => {
            actualSets.head collect {
              case (ids, value) if op1.pf.isDefinedAt(value) =>
                (ids, op1.pf(value))
            }
          }
          
          case Op2Binding(op2) =>
            handleBinary(actualSets(0), actuals(0).provenance, actualSets(1), actuals(1).provenance)(op2.pf)
          
          case _ => sys.error("todo")
        }
      }
      
      case Cond(_, pred, left, right) => {
        val packed = handleBinary(loopForJoin(env, restrict)(pred), pred.provenance, loopForJoin(env, restrict)(left), left.provenance) {
          case (b: JBool, right) => JArray(b :: right :: Nil)
        }
        
        handleBinary(packed, unifyProvenance(expr.relations)(pred.provenance, left.provenance).get, loopForJoin(env, restrict)(right), right.provenance) {
          case (JArray(JBool(pred) :: left :: Nil), right) =>
            if (pred) left else right
        }
      }
      
      case Where(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (value, JTrue) => value
        }
      }
      
      case With(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JObject(leftFields), JObject(rightFields)) =>
            JObject(leftFields ++ rightFields)
        }
      }
      
      case Union(_, left, right) => {
        val leftSorted = loop(env, restrict)(left).sorted(SEOrder.toScalaOrdering)
        val rightSorted = loop(env, restrict)(right).sorted(SEOrder.toScalaOrdering)
        
        mergeAlign(leftSorted, rightSorted)(SEOrder.order)
      }
      
      case Intersect(_, left, right) => {
        val leftSorted = loop(env, restrict)(left).sorted(SEOrder.toScalaOrdering)
        val rightSorted = loop(env, restrict)(right).sorted(SEOrder.toScalaOrdering)
        
        zipAlign(leftSorted, rightSorted)(SEOrder.order) map {
          case (se, _) => se
        }
      }
      
      case Difference(_, left, right) => {
        val leftSorted = loop(env, restrict)(left).sorted(SEOrder.toScalaOrdering)
        val rightSorted = loop(env, restrict)(right).sorted(SEOrder.toScalaOrdering)
        
        biasLeftAlign(leftSorted, rightSorted)(SEOrder.order)
      }
      
      case Add(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN + rightN)
        }
      }
      
      case Sub(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN - rightN)
        }
      }
      
      case Mul(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN * rightN)
        }
      }
      
      case Div(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN / rightN)
        }
      }
      
      case Mod(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN % rightN)
        }
      }
      
      case Pow(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JNum(leftN pow rightN.toInt)
        }
      }
      
      case Lt(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN < rightN)
        }
      }
      
      case LtEq(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN <= rightN)
        }
      }
      
      case Gt(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN > rightN)
        }
      }
      
      case GtEq(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JNum(leftN), JNum(rightN)) => JBool(leftN >= rightN)
        }
      }
      
      case Eq(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (leftV, rightV) => JBool(leftV == rightV)
        }
      }
      
      case NotEq(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (leftV, rightV) => JBool(leftV != rightV)
        }
      }
      
      case And(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JBool(leftB), JBool(rightB)) => JBool(leftB && rightB)
        }
      }
      
      case Or(_, left, right) => {
        handleBinary(loopForJoin(env, restrict)(left), left.provenance, loopForJoin(env, restrict)(right), right.provenance) {
          case (JBool(leftB), JBool(rightB)) => JBool(leftB || rightB)
        }
      }
      
      case Comp(_, child) => {
        loop(env, restrict)(child) collect {
          case (ids, JBool(value)) => (ids, JBool(!value))
        }
      }
      
      case Neg(_, child) => {
        loop(env, restrict)(child) collect {
          case (ids, JNum(value)) => (ids, JNum(-value))
        }
      }
      
      case Paren(_, child) => loop(env, restrict)(child)
    }
    
    def loopForJoin(env: Map[(Let, String), Dataset], restrict: Map[Provenance, Set[Identities]])(expr: Expr): Dataset = {
      val back = loop(env, restrict)(expr)
      
      restrict get expr.provenance map { idx =>
        back filter {
          case (ids, _) => idx(ids)
        }
      } getOrElse back
    }
    
    def handleBinary(left: Dataset, leftProv: Provenance, right: Dataset, rightProv: Provenance)(pf: PartialFunction[(JValue, JValue), JValue]): Dataset = {
      val intersected = leftProv.possibilities intersect rightProv.possibilities filter { p => p != ValueProvenance && p != NullProvenance }
      
      if (intersected.isEmpty)
        cross(left, right)(pf)
      else
        join(left, leftProv, right, rightProv)(pf)
    }
    
    def join(left: Dataset, leftProv: Provenance, right: Dataset, rightProv: Provenance)(pf: PartialFunction[(JValue, JValue), JValue]): Dataset = {
      val linearLeft = linearProvPossibilities(leftProv)
      val posLeft = leftProv.possibilities
      
      val linearRight = linearProvPossibilities(rightProv)
      val posRight = rightProv.possibilities
      
      val indicesLeft = linearLeft.zipWithIndex collect {
        case (p, i) if posRight(p) => i
      }
      
      val orderLeft = orderFromIndices(indicesLeft)
      
      val indicesRight = linearRight.zipWithIndex collect {
        case (p, i) if posLeft(p) => i
      }
      
      val orderRight = orderFromIndices(indicesRight)
      
      val leftMergeKey = 0 until linearLeft.length map { Left(_) } toList
      
      val rightMergeKey = linearRight.zipWithIndex collect {
        case (p, i) if !posLeft(p) => Right(i)
      }
      
      val mergeKey: List[Either[Int, Int]] = leftMergeKey ++ rightMergeKey
      
      val leftSorted = if (indicesLeft == (0 until indicesLeft.length))
        left
      else
        left sorted orderLeft.toScalaOrdering     // must be stable!
      
      val rightSorted = if (indicesRight == (0 until indicesRight.length))
        right
      else
        right sorted orderRight.toScalaOrdering     // must be stable!
      
      val joined = zipAlign(leftSorted, rightSorted) {
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
    
    def cross(left: Dataset, right: Dataset)(pf: PartialFunction[(JValue, JValue), JValue]): Dataset = {
      for {
        (idsLeft, leftV) <- left
        (idsRight, rightV) <- right
        
        if pf.isDefinedAt((leftV, rightV))
      } yield (idsLeft ++ idsRight, pf((leftV, rightV)))
    }
    
    if (expr.errors.isEmpty) {
      loop(Map(), Map())(expr) map {
        case (_, value) => value
      }
    } else {
      Seq.empty
    }
  }
  
  private def linearProvPossibilities(prov: Provenance): List[Provenance] = {
    def loop(prov: Provenance): List[Provenance] = prov match {
      case ProductProvenance(left, right) =>
        loop(left) ++ loop(right)
      
      case CoproductProvenance(left, right) => {
        val leftRec = loop(left)
        val rightRec = loop(right)
        
        val merged = leftRec zip rightRec map {
          case (l, r) => CoproductProvenance(l, r)
        }
        
        
        merged ++ (leftRec drop merged.length) ++ (rightRec drop merged.length)
      }
      
      case prov => prov :: Nil
    }
    
    val (_, back) = loop(prov).foldLeft((Set[Provenance](), List[Provenance]())) {
      case ((acc, result), prov) if acc(prov) => (acc, result)
      case ((acc, result), prov) => (acc + prov, prov :: result)
    }
    
    back.reverse
  }
  
  private def orderFromIndices(indices: Seq[Int]): Order[SEvent] = {
    new Order[SEvent] {
      def order(left: SEvent, right: SEvent) = {
        (indices map left._1) zip (indices map right._1) map {
          case (l, r) => Ordering.fromInt(l - r)
        } reduce { _ |+| _ }
      }
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
  
  /**
   * Poor-man's cogroup specialized on the left/right cases
   */
  private def mergeAlign[A](left: Seq[A], right: Seq[A])(f: (A, A) => Ordering): Seq[A] = {
    if (left.isEmpty) {
      right
    } else if (right.isEmpty) {
      left
    } else {
      f(left.head, right.head) match {
        case Ordering.EQ => left.head +: mergeAlign(left.tail, right.tail)(f)
        case Ordering.LT => left.head +: mergeAlign(left.tail, right)(f)
        case Ordering.GT => right.head +: mergeAlign(left, right.tail)(f)
      }
    }
  }
  
  /**
   * Poor-man's cogroup specialized on the left case
   */
  private def biasLeftAlign[A](left: Seq[A], right: Seq[A])(f: (A, A) => Ordering): Seq[A] = {
    if (left.isEmpty) {
      left
    } else if (right.isEmpty) {
      left
    } else {
      f(left.head, right.head) match {
        case Ordering.EQ => biasLeftAlign(left.tail, right.tail)(f)
        case Ordering.LT => left.head +: biasLeftAlign(left.tail, right)(f)
        case Ordering.GT => biasLeftAlign(left, right.tail)(f)
      }
    }
  }
  
  // no need for biasRightAlign, since difference biases to the left
}
