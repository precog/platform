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
package com.reportgrid.storage

import Reified._

sealed trait Expr[A] {
  def reified: Reified[A]
}

sealed trait ExprBinary[A, B, C] extends Expr[B] {
  def left: Expr[A]
  def right: Expr[A]
  def op: C
}
sealed trait ExprUnary[A, B, C] extends Expr[B] {
  def value: Expr[A]
  def op: C
}
sealed trait ExprConstant[A] extends Expr[A] {
  def value: A
}
sealed trait ExprRef[A] extends Expr[A] {
  def id: String
}
sealed trait ExprComparablePimp[A] {
  def value: Expr[A]

  private implicit val reifiedImplicit = value.reified

  def > (that: Expr[A]) = ExprBool.BinaryCmp(value, that, ExprBool.BinaryCmpOperators.GT)

  def >= (that: Expr[A]) = ExprBool.BinaryCmp(value, that, ExprBool.BinaryCmpOperators.GTE)

  def < (that: Expr[A]) = ExprBool.BinaryCmp(value, that, ExprBool.BinaryCmpOperators.LT)

  def <= (that: Expr[A]) = ExprBool.BinaryCmp(value, that, ExprBool.BinaryCmpOperators.LTE)

  def === (that: Expr[A]) = ExprBool.BinaryCmp(value, that, ExprBool.BinaryCmpOperators.EQ)

  def != (that: Expr[A]) = ExprBool.BinaryCmp(value, that, ExprBool.BinaryCmpOperators.NEQ)
}

trait ExprDataset[I, A] extends Expr[Dataset[I, A]]

trait ExprTuple2[A, B] extends Expr[(A, B)]

sealed case class ExprOptionPimp[A: Reified](value: Expr[Option[A]]) {
  import ExprOption._

  def fold[Z: Reified](ifNone: Expr[Z], ifSome: Expr[A] => Expr[Z]) = Fold(value, ifNone, ifSome)

  def getOrElse(default: Expr[A]) = fold(default, identity[Expr[A]])
}
object ExprOption {
  sealed case class Fold[A: Reified, Z: Reified](value: Expr[Option[A]], ifNone: Expr[Z], ifSome: Expr[A] => Expr[Z]) 
      extends Expr[Option[A]] {
    def reified = Reified.OptionT[A]
  }
}

sealed case class ExprBoolPimp(value: Expr[Boolean]) {
  import ExprBool._

  def unary_! = Unary(value, UnaryOperators.Not)

  def & (that: Expr[Boolean]) = Binary(value, that, BinaryOperators.And)

  def | (that: Expr[Boolean]) = Binary(value, that, BinaryOperators.Or)

  def ^ (that: Expr[Boolean]) = Binary(value, that, BinaryOperators.Xor)
}
object ExprBool {
  private[ExprBool] trait ExprBool extends Expr[Boolean] {
    def reified = Reified.BooleanT
  }

  sealed trait BinaryOperator
  object BinaryOperators {
    case object And extends BinaryOperator
    case object Or  extends BinaryOperator
    case object Xor extends BinaryOperator
  }

  sealed trait BinaryCmpOperator
  object BinaryCmpOperators {
    case object GT  extends BinaryCmpOperator
    case object GTE extends BinaryCmpOperator
    case object LT  extends BinaryCmpOperator
    case object LTE extends BinaryCmpOperator
    case object EQ  extends BinaryCmpOperator
    case object NEQ extends BinaryCmpOperator
  }

  sealed trait UnaryOperator
  object UnaryOperators {
    case object Not extends UnaryOperator
  }

  sealed case class Binary(left: Expr[Boolean], right: Expr[Boolean], op: BinaryOperator) extends 
    ExprBool with ExprBinary[Boolean, Boolean, BinaryOperator]

  sealed case class Unary(value: Expr[Boolean], op: UnaryOperator) extends 
    ExprBool with ExprUnary[Boolean, Boolean, UnaryOperator]

  sealed case class BinaryCmp[A: Reified](left: Expr[A], right: Expr[A], op: BinaryCmpOperator) extends 
    ExprBool with ExprBinary[A, Boolean, BinaryCmpOperator]

  sealed case class Constant(value: Boolean) extends ExprBool with ExprConstant[Boolean]

  sealed case class Ref(id: String) extends ExprBool with ExprRef[Boolean]

  object True extends Constant(true)

  object False extends Constant(false)
}

sealed case class ExprLongPimp(value: Expr[Long]) extends ExprComparablePimp[Long] {
  import ExprLong._

  def unary_- = Unary(value, UnaryOperators.Negate)

  def + (that: Expr[Long]) = Binary(value, that, BinaryOperators.Add)

  def - (that: Expr[Long]) = Binary(value, that, BinaryOperators.Sub)

  def * (that: Expr[Long]) = Binary(value, that, BinaryOperators.Mul)

  def / (that: Expr[Long]) = Binary(value, that, BinaryOperators.Div)
}
object ExprLong {
  private[ExprLong] trait ExprLong extends Expr[Long] {
    def reified = Reified.LongT
  }

  sealed trait BinaryOperator
  object BinaryOperators {
    case object Add extends BinaryOperator
    case object Sub extends BinaryOperator
    case object Mul extends BinaryOperator
    case object Div extends BinaryOperator
  }

  sealed trait UnaryOperator
  object UnaryOperators {
    case object Negate extends UnaryOperator
  }

  sealed case class Binary(left: Expr[Long], right: Expr[Long], op: BinaryOperator) extends 
    ExprLong with ExprBinary[Long, Long, BinaryOperator]

  sealed case class Unary(value: Expr[Long], op: UnaryOperator) extends 
    ExprLong with ExprUnary[Long, Long, UnaryOperator]

  sealed case class Constant(value: Long) extends ExprLong with ExprConstant[Long]

  sealed case class Ref(id: String) extends ExprLong with ExprRef[Long]

  object Zero extends Constant(0)
}

sealed case class ExprTuple2Pimp[A: Reified, B: Reified](value: Expr[(A, B)]) {
  def _1 = ExprTuple._1(value)

  def _2 = ExprTuple._2(value)
}
object ExprTuple {
  case class _1[A: Reified, B: Reified](value: Expr[(A, B)]) extends Expr[A] {
    def reified = implicitly[Reified[A]]
  }
  case class _2[A: Reified, B: Reified](value: Expr[(A, B)]) extends Expr[B] {
    def reified = implicitly[Reified[B]]
  }
}

sealed case class ExprDatasetPimp[I: Reified, A: Reified](value: ExprDataset[I, A]) {
  /** Returns a dataset whose identities are equal to the values.
   */
  def values: ExprDataset[A, A] = ExprDataset.Values(value)

  def filter(f: Expr[A] => Expr[Boolean]): ExprDataset[I, A] = ExprDataset.Filter(value, f)

  def map[B: Reified](f: Expr[A] => Expr[B]): ExprDataset[I, B] = ExprDataset.Map(value, f)

  def cross[IB: Reified, B: Reified](that: ExprDataset[IB, B]): ExprDataset[(I, IB), (A, B)] = 
    ExprDataset.Cross(value, that)

  def join[B: Reified](that: ExprDataset[I, B]): ExprDataset[I, (A, B)] = 
    ExprDataset.Join(value, that)

  def cogroup[B: Reified](that: ExprDataset[I, B]): ExprDataset[I, (Option[A], Option[B])] = 
    ExprDataset.Cogroup(value, that)

  def union(that: ExprDataset[I, A]): ExprDataset[I, A] = ExprDataset.Union(value, that)

  def intersect(that: ExprDataset[I, A]): ExprDataset[I, A] = ExprDataset.Intersect(value, that)
}
object ExprDataset {
  case class Filter[I: Reified, A: Reified](value: ExprDataset[I, A], f: Expr[A] => Expr[Boolean]) 
      extends ExprDataset[I, A] {
    def reified = Reified.DatasetT[I, A]
  }
  case class Map[I: Reified, A: Reified, B: Reified](value: ExprDataset[I, A], f: Expr[A] => Expr[B]) 
      extends ExprDataset[I, B] {
    def reified = Reified.DatasetT[I, B]
  }
  case class Cross[I: Reified, A: Reified, IB: Reified, B: Reified](v1: ExprDataset[I, A], v2: ExprDataset[IB, B]) 
      extends ExprDataset[(I, IB), (A, B)] {
    def reified = Reified.DatasetT[(I, IB), (A, B)]
  }
  case class Join[I: Reified, A: Reified, B: Reified](v1: ExprDataset[I, A], v2: ExprDataset[I, B]) 
      extends ExprDataset[I, (A, B)] {
    def reified = Reified.DatasetT[I, (A, B)]
  }
  case class Cogroup[I: Reified, A: Reified, B: Reified](v1: ExprDataset[I, A], v2: ExprDataset[I, B]) 
      extends ExprDataset[I, (Option[A], Option[B])] {
    def reified = Reified.DatasetT[I, (Option[A], Option[B])]  
  }
  case class Union[I: Reified, A: Reified](v1: ExprDataset[I, A], v2: ExprDataset[I, A]) 
      extends ExprDataset[I, A] {
    def reified = Reified.DatasetT[I, A]
  }
  case class Intersect[I: Reified, A: Reified](v1: ExprDataset[I, A], v2: ExprDataset[I, A]) 
      extends ExprDataset[I, A] {
    def reified = Reified.DatasetT[I, A]
  }

  case class Values[I: Reified, A: Reified](value: ExprDataset[I, A]) extends ExprDataset[A, A] {
    def reified = Reified.DatasetT[A, A]
  }

  case class Ref[I: Reified, A: Reified](id: String) extends ExprDataset[I, A] with ExprRef[Dataset[I, A]] {
    def reified = Reified.DatasetT[I, A]
  }

  case class Load[I: Reified, A: Reified](source: DatasetId) extends ExprDataset[I, A] {
    def reified = Reified.DatasetT[I, A]
  }
}