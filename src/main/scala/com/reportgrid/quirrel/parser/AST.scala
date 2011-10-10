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
package parser

trait AST { outer => 
  def root: Expr
  
  sealed trait Expr {
    def root = outer.root
  }
  
  case class Binding(id: String, params: Vector[Formal], body: Expr, in: Expr) extends Expr
  
  case class New(expr: Expr) extends Expr
  
  case class Relate(from: Expr, to: Expr, in: Expr) extends Expr
  
  case class Var(id: String) extends Expr
  
  case class TicVar(id: String) extends Expr
  
  case class StrLit(value: String) extends Expr
  case class NumLit(value: String) extends Expr
  case class BoolLit(value: Boolean) extends Expr
  
  case class ObjectDef(props: Vector[(String, Expr)]) extends Expr
  case class ArrayDef(values: Vector[Expr]) extends Expr
  
  case class Descent(body: Expr, property: String) extends Expr
  
  case class Deref(body: Expr, index: Expr) extends Expr
  
  case class Dispatch(name: String, actuals: Vector[String]) extends Expr
  
  case class Operation(left: Expr, op: String, right: Expr) extends Expr
  
  case class Add(left: Expr, right: Expr) extends Expr
  case class Sub(left: Expr, right: Expr) extends Expr
  case class Mult(left: Expr, right: Expr) extends Expr
  case class Div(left: Expr, right: Expr) extends Expr
  
  case class Lt(left: Expr, right: Expr) extends Expr
  case class LtEq(left: Expr, right: Expr) extends Expr
  case class Gt(left: Expr, right: Expr) extends Expr
  case class GtEq(left: Expr, right: Expr) extends Expr
  
  case class Eq(left: Expr, right: Expr) extends Expr
  case class NotEq(left: Expr, right: Expr) extends Expr
  
  case class Comp(body: Expr) extends Expr
  case class Neg(body: Expr) extends Expr
  
  case class Paren(body: Expr) extends Expr
  
  case class Formal(name: String)
}
