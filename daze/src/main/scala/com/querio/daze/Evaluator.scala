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
package com.querio
package daze

import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._
import IterateeT._

import com.reportgrid.analytics.Path
import com.reportgrid.yggdrasil._
import com.reportgrid.util._

trait Evaluator extends DAG with CrossOrdering with OperationsAPI {
  import instructions._
  import dag._

  private implicit object IdentitiesOrder extends Order[SEvent] {
    def order(e1: SEvent, e2: SEvent) = {
      val (ids1, _) = e1
      val (ids2, _) = e2
      ids1.zip(ids2).foldLeft[Ordering](Ordering.EQ) {
        case (Ordering.EQ, (i1, i2)) => Ordering.fromInt((i1 - i2) toInt)
        case (acc, _) => acc
      }
    }
  }
  
  def eval[X](graph: DepGraph): DatasetEnum[X, SEvent, IO] = {
    def loop(graph: DepGraph, roots: List[DatasetEnum[X, SEvent, IO]]): DatasetEnum[X, SEvent, IO] = graph match {
      case SplitRoot(_, depth) => roots(depth)
      
      case Root(_, instr) => {
        val sev = instr match {
          case PushString(str) => SString(str)
          case PushNum(str) => SDecimal(BigDecimal(str))
          case PushTrue => SBoolean(true)
          case PushFalse => SBoolean(false)
          case PushObject => SObject(Map())
          case PushArray => SArray(Vector())
        }
        
        ops.point((Vector(), sev))
      }
      
      case dag.New(_, parent) => loop(parent, roots)
      
      case dag.LoadLocal(_, _, parent, _) => {
        ops.flatMap(loop(parent, roots)) {
          case (_, SString(str)) => query.fullProjection(Path(str))
          case _ => ops.empty[X, SEvent, IO]
        }
      }
      
      case Operate(_, Comp, parent) => {
        val enum = loop(parent, roots)
        
        ops.mapOpt(enum) {
          case (id, SBoolean(b)) => Some((id, SBoolean(!b)))
          case _ => None
        }
      }
      
      case Operate(_, Neg, parent) => {
        val enum = loop(parent, roots)
        
        ops.mapOpt(enum) {
          case (id, SDecimal(d)) => Some((id, SDecimal(-d)))
          case _ => None
        }
      }
      
      case Operate(_, WrapArray, parent) => {
        val enum = loop(parent, roots)
        
        ops.map(enum) {
          case (id, sv) => (id, SArray(Vector(sv)))
        }
      }
      
      case dag.Reduce(_, red, parent) => {
        val enum = loop(parent, roots).enum

        val reducedEnumP: EnumeratorP[X, SEvent, IO] = new EnumeratorP[X, SEvent, IO] {
          override def apply[F[_[_], _], A](implicit mt: MonadTrans[F]) = (step: StepT[X, SEvent, ({type λ[α] = F[IO, α] })#λ, A]) => {
            type FIO[α] = F[IO, α]
            type EnumeratorM[α] = EnumeratorT[X, SEvent, FIO, α]
            implicit val FMonad: Monad[FIO] = mt[IO]

            for {
              opt <- reductionIter[X, F](red) >>== enum[F, Option[SValue]]
              a   <- step.pointI >>== (opt.map(sv => EnumeratorT.enumOne[X, SEvent, FIO, A](Vector(), sv)).getOrElse(PlusEmpty[EnumeratorM].empty[A]))
            } yield a
          }
        }

        DatasetEnum[X, SEvent, IO](reducedEnumP)
      }
      
      case dag.Split(_, parent, child) => {
        val splitEnum = loop(parent, roots)
        
        ops.flatMap(splitEnum) {
          case (_, sv) => loop(child, ops.point[X, SEvent, IO]((Vector(), sv)) :: roots)
        }
      }
      
      case Join(_, instr, left, right) => {
        val leftEnum = loop(left, roots)
        val rightEnum = loop(right, roots)
        
        val (pairs, op, distinct) = instr match {
          case Map2Match(op) => (ops.join(leftEnum, rightEnum), op, true)
          case Map2Cross(op) => (ops.crossLeft(leftEnum, rightEnum), op, false)
          case Map2CrossLeft(op) => (ops.crossLeft(leftEnum, rightEnum), op, false)
          case Map2CrossRight(op) => (ops.crossRight(leftEnum, rightEnum), op, false)
        }
        
        ops.mapOpt(pairs) {
          case ((ids1, sv1), (ids2, sv2)) => {
            val ids = if (distinct)
              (ids1 ++ ids2).distinct
            else
              ids1 ++ ids2
            
            binaryOp(op)(sv1, sv2) map { sv => (ids, sv) }
          }
        }
      }
      
      case Filter(_, cross, _, target, boolean) => {
        val targetEnum = loop(target, roots)
        val booleanEnum = loop(boolean, roots)
        
        val (pairs, distinct) = cross match {
          case None => (ops.join(targetEnum, booleanEnum), true)
          case Some(CrossNeutral) => (ops.crossLeft(targetEnum, booleanEnum), false)
          case Some(CrossLeft) => (ops.crossLeft(targetEnum, booleanEnum), false)
          case Some(CrossRight) => (ops.crossRight(targetEnum, booleanEnum), false)
        }
        
        ops.mapOpt(pairs) {
          case ((ids1, sv), (ids2, SBoolean(true))) => {
            val ids = if (distinct)
              (ids1 ++ ids2).distinct
            else
              ids1 ++ ids2
            
            Some((ids, sv))
          }
          
          case _ => None
        }
      }
      
      case Sort(parent, indexes) =>
        ops.sort(loop(parent, roots), indexes)
    }
    
    loop(orderCrosses(graph), Nil)
  }
  
  private def reductionIter[X, F[_[_], _]: MonadTrans](red: Reduction): IterateeT[X, SEvent, ({ type λ[α] = F[IO, α] })#λ, Option[SValue]] = sys.error("no reductions implemented yet...")
  
  private def binaryOp(op: BinaryOperation): (SValue, SValue) => Option[SValue] = {
    import Function._
    
    def add(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = Some(left + right)
    def sub(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = Some(left - right)
    def mul(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = Some(left * right)
    
    def div(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = {
      if (right == 0)
        None
      else
        Some(left / right)
    }
    
    def lt(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left < right)
    def lteq(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left <= right)
    def gt(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left > right)
    def gteq(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left >= right)
    
    def eq(left: SValue, right: SValue): Option[Boolean] = Some(left == right)
    def neq(left: SValue, right: SValue): Option[Boolean] = Some(left != right)
    
    def and(left: Boolean, right: Boolean): Option[Boolean] = Some(left && right)
    def or(left: Boolean, right: Boolean): Option[Boolean] = Some(left || right)
    
    def joinObject(left: Map[String, SValue], right: Map[String, SValue]) = Some(left ++ right)
    def joinArray(left: Vector[SValue], right: Vector[SValue]) = Some(left ++ right)
    
    def coerceNumerics(pair: (SValue, SValue)): Option[(BigDecimal, BigDecimal)] = pair match {
      case (SDecimal(left), SDecimal(right)) => Some((left, right))
      case _ => None
    }
    
    def coerceBooleans(pair: (SValue, SValue)): Option[(Boolean, Boolean)] = pair match {
      case (SBoolean(left), SBoolean(right)) => Some((left, right))
      case _ => None
    }
    
    def coerceObjects(pair: (SValue, SValue)): Option[(Map[String, SValue], Map[String, SValue])] = pair match {
      case (SObject(left), SObject(right)) => Some((left, right))
      case _ => None
    }
    
    def coerceArrays(pair: (SValue, SValue)): Option[(Vector[SValue], Vector[SValue])] = pair match {
      case (SArray(left), SArray(right)) => Some((left, right))
      case _ => None
    }
    
    op match {
      case Add => untupled((coerceNumerics _) andThen { _ flatMap (add _).tupled map SDecimal })
      case Sub => untupled((coerceNumerics _) andThen { _ flatMap (sub _).tupled map SDecimal })
      case Mul => untupled((coerceNumerics _) andThen { _ flatMap (mul _).tupled map SDecimal })
      case Div => untupled((coerceNumerics _) andThen { _ flatMap (div _).tupled map SDecimal })
      
      case Lt => untupled((coerceNumerics _) andThen { _ flatMap (lt _).tupled map SBoolean })
      case LtEq => untupled((coerceNumerics _) andThen { _ flatMap (lteq _).tupled map SBoolean })
      case Gt => untupled((coerceNumerics _) andThen { _ flatMap (gt _).tupled map SBoolean })
      case GtEq => untupled((coerceNumerics _) andThen { _ flatMap (gteq _).tupled map SBoolean })
      
      case Eq => untupled((eq _).tupled andThen { _ map SBoolean })
      case NotEq => untupled((neq _).tupled andThen { _ map SBoolean })
      
      case And => untupled((coerceBooleans _) andThen { _ flatMap (and _).tupled map SBoolean })
      case Or => untupled((coerceBooleans _) andThen { _ flatMap (or _).tupled map SBoolean })
      
      case WrapObject => {
        case (SString(key), value) => Some(SObject(Map(key -> value)))
        case _ => None
      }
      
      case JoinObject => untupled((coerceObjects _) andThen { _ flatMap (joinObject _).tupled map SObject })
      case JoinArray => untupled((coerceArrays _) andThen { _ flatMap (joinArray _).tupled map SArray })
      
      case ArraySwap => {
        case (SDecimal(index), SArray(arr)) if index.isValidInt => {
          val i = index.toInt - 1
          if (i < 0 || i >= arr.length) {
            None
          } else {
            val (left, right) = arr splitAt i
            Some(SArray(left.init ++ Vector(right.head, left.last) ++ left.tail))
          }
        }
        
        case _ => None
      }
      
      case DerefObject => {
        case (SString(key), SObject(obj)) => obj get key
        case _ => None
      }
      
      case DerefArray => {
        case (SDecimal(index), SArray(arr)) if index.isValidInt =>
          arr.lift(index.toInt)
        
        case _ => None
      }
    }
  }
}
