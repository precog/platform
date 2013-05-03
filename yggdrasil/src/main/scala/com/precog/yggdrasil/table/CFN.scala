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
package com.precog.yggdrasil
package table

import blueeyes.json._
import com.precog.bytecode.JType
import com.precog.common._

sealed trait CFId
case class LeafCFId(identity: String) extends CFId
case class ComposedCFId(l: CFId, r: CFId) extends CFId
case class PartialLeftCFId(cv: CValue, r: CFId) extends CFId
case class PartialRightCFId(l: CFId, cv: CValue) extends CFId

object CFId {
  def apply(identity: String) = LeafCFId(identity)
}

trait CF {
  def identity: CFId
  override final def equals(other: Any): Boolean = other match {
    case cf: CF => identity == cf.identity 
    case _ => false
  }
  
  override final def hashCode: Int = identity.hashCode
  
  override def toString() = identity.toString  
}

trait CF1 extends CF { self =>
  def apply(c: Column): Option[Column]
  def apply(cv: CValue): Option[Column] = apply(Column.const(cv))

  // Do not use PartialFunction.compose or PartialFunction.andThen for composition,
  // because they will fail out with MatchError.
  def compose(f1: CF1): CF1 = new CF1 {
    def apply(c: Column) = f1(c).flatMap(self.apply)
    val identity = ComposedCFId(f1.identity, self.identity)
  }

  def andThen(f1: CF1): CF1 = new CF1 {
    def apply(c: Column) = self.apply(c).flatMap(f1.apply)
    val identity = ComposedCFId(self.identity, f1.identity)
  }
}

object CF1 {
  def apply(name: String)(f: Column => Option[Column]): CF1 = apply(CFId(name))(f)
  def apply(id: CFId)(f: Column => Option[Column]): CF1 = new CF1 {
    def apply(c: Column) = f(c)
    val identity = id
  }
}

object CF1P {
  def apply(name: String)(f: PartialFunction[Column, Column]): CF1 = apply(CFId(name))(f)
  def apply(id: CFId)(f: PartialFunction[Column, Column]): CF1 = new CF1 {
    def apply(c: Column) = f.lift(c)
    val identity = id
  }
}

trait CF2 extends CF { self =>
  def apply(c1: Column, c2: Column): Option[Column] 
  
  @inline
  def partialLeft(cv: CValue): CF1 = {
    new CF1 {
      def apply(c2: Column) = self.apply(Column.const(cv), c2)
      val identity = PartialLeftCFId(cv, self.identity)
    }
  }
  
  @inline
  def partialRight(cv: CValue): CF1 = {
    new CF1 {
      def apply(c1: Column) = self.apply(c1, Column.const(cv))
      val identity = PartialRightCFId(self.identity, cv)
    }
  }
}

object CF2 {
  def apply(id: String)(f: (Column, Column) => Option[Column]): CF2 = apply(CFId(id))(f)
  def apply(id: CFId)(f: (Column, Column) => Option[Column]): CF2 = new CF2 {
    def apply(c1: Column, c2: Column) = f(c1, c2)
    val identity = id
  }
}

object CF2P {
  def apply(id: String)(f: PartialFunction[(Column, Column), Column]): CF2 = apply(CFId(id))(f)
  def apply(id: CFId)(f: PartialFunction[(Column, Column), Column]): CF2 = new CF2 {
    def apply(c1: Column, c2: Column) = f.lift((c1, c2))
    val identity = id
  }
}

trait CScanner[M[+_]] {
  type A
  def init: A
  def scan(a: A, cols: Map[ColumnRef, Column], range: Range): M[(A, Map[ColumnRef, Column])]
}

trait CSchema {
  def columnRefs: Set[ColumnRef]
  def columns(jtype: JType): Set[Column]
}

trait CReducer[A] {
  def reduce(schema: CSchema, range: Range): A
}


/* ctags
type FN */
