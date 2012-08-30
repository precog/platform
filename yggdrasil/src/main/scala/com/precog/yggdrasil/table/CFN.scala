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

class CF1(f: Column => Option[Column]) { //extends (Column => Option[Column]) {
  def apply(c: Column): Option[Column] = f(c)

  // Do not use PartialFunction.compose or PartialFunction.andThen for composition,
  // because they will fail out with MatchError.
  def compose(f1: CF1): CF1 = new CF1(c => f1(c).flatMap(apply))
  def andThen(f1: CF1): CF1 = new CF1(c => this(c).flatMap(f1.apply))
}

class CF1P(f: PartialFunction[Column, Column]) extends CF1(f.lift) 

class CF2(f: (Column, Column) => Option[Column]) { // extends ((Column, Column) => Option[Column]) {
  def apply(c1: Column, c2: Column): Option[Column] = f(c1, c2)
  
  @inline
  def partialLeft(cv: CValue): CF1 = {
    val c1 = Column.const(cv)
    new CF1({ c2 => apply(c1, c2) })
  }
  
  @inline
  def partialRight(cv: CValue): CF1 = {
    val c2 = Column.const(cv)
    new CF1({ c1 => apply(c1, c2) })
  }
}

class CF2P(f: PartialFunction[(Column, Column), Column]) extends CF2(Function.untupled(f.lift))

trait CScanner {
  type A
  def init: A
  def scan(a: A, cols: Set[Column], range: Range): (A, Set[Column])
}

trait CReducer[A] {
  def reduce(columns: (JType => Set[Column]), range: Range): A
}


/* ctags
type FN */
