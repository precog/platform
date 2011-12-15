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
package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.quirrel.typer.Binder
import com.querio.bytecode.{Instructions}

import scalaz._

trait Emitter extends AST with Instructions with Binder {
  import instructions._

  def emit(expr: Expr): Vector[Instruction] = {
    def emit0(expr: Expr, vector: Vector[Instruction]): Vector[Instruction] = {
      // vector ++ 

      (expr match {
        case ast.New(loc, child) => 
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
        
        case t @ ast.TicVar(loc, id) => 
        
        case ast.StrLit(loc, value) => 
          Vector(PushString(value))
        
        case ast.NumLit(loc, value) => 
          Vector(PushNum(value))
        
        case ast.BoolLit(loc, value) => 
          value match {
            case true  => PushTrue
            case false => PushFalse
          }
        
        case ast.ObjectDef(loc, props) => 
        
        case ast.ArrayDef(loc, values) => 
        
        case ast.Descent(loc, child, property) => 
        
        case ast.Deref(loc, left, right) => 
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case BuiltIn(BuiltIns.Load.name, arity) =>
              emit(actuals.head) :+ LoadLocal(Het)

            case BuiltIn(n, arity) =>

            case UserDef(e) =>

            case NullBinding => 
          }
        
        case ast.Operation(loc, left, op, right) => 
        
        case ast.Add(loc, left, right) => 
        
        case ast.Sub(loc, left, right) => 

        case ast.Mul(loc, left, right) => 
        
        case ast.Div(loc, left, right) => 
        
        case ast.Lt(loc, left, right) => 
        
        case ast.LtEq(loc, left, right) => 
        
        case ast.Gt(loc, left, right) => 
        
        case ast.GtEq(loc, left, right) => 
        
        case ast.Eq(loc, left, right) => 
        
        case ast.NotEq(loc, left, right) => 
        
        case ast.Or(loc, left, right) => 
        
        case ast.And(loc, left, right) =>
        
        case ast.Comp(loc, child) =>
        
        case ast.Neg(loc, child) => 
        
        case ast.Paren(loc, child) => 
      })

      null
    }

    emit0(expr, Vector.empty)
  }
}