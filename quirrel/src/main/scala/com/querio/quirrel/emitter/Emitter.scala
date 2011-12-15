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
import com.querio.quirrel.typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import com.querio.bytecode.{Instructions}

import scalaz.{Validation, Success, Failure}
import scalaz.Scalaz._

trait Emitter extends AST with Instructions with Binder with ProvenanceChecker {
  import instructions._
  case class EmitterError(expr: Expr, message: String) extends Exception(message)

  type EmitterType = Validation[EmitterError, Vector[Instruction]]

  private def nullProvenanceError(expr: Expr): EmitterType = Failure(EmitterError(expr, "Expression has null provenance"))
  private def notImpl(expr: Expr): EmitterType = Failure(EmitterError(expr, "Not implemented"))

  def emit(expr: Expr): EmitterType = {
    def emitBinary(left: Expr, right: Expr, op: BinaryOperation): EmitterType = {
      (left.provenance, right.provenance) match {
        case (NullProvenance, _) => 
          nullProvenanceError(left)

        case (_, NullProvenance) => 
          nullProvenanceError(right)

        case (p1, p2) =>
          val bytecode = (p1, p2) match {
            case (StaticProvenance(p1), StaticProvenance(p2)) if (p1 == p2) => 
              Map2Match(op)
            
            case (DynamicProvenance(id1), DynamicProvenance(id2)) if (id1 == id2) =>
              Map2Match(op)

            case (_, _) =>
              Map2Cross(op)
          }

          for {
            leftInstr   <- emit(left)
            rightInstr  <- emit(right)
          } yield (leftInstr ++ rightInstr) :+ bytecode
      }
    }

    def emit0(expr: Expr, vector: Vector[Instruction]): EmitterType = {
      ((expr match {
        case ast.New(loc, child) => 
          notImpl(expr)
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          notImpl(expr)
        
        case t @ ast.TicVar(loc, id) => 
          notImpl(expr)
        
        case ast.StrLit(loc, value) => 
          Success(Vector(PushString(value)))
        
        case ast.NumLit(loc, value) => 
          Success(Vector(PushNum(value)))
        
        case ast.BoolLit(loc, value) => 
          Success(value match {
            case true  => Vector(PushTrue)
            case false => Vector(PushFalse)
          })
        
        case ast.ObjectDef(loc, props) => 
          notImpl(expr)
        
        case ast.ArrayDef(loc, values) => 
          notImpl(expr)
        
        case ast.Descent(loc, child, property) => 
          notImpl(expr)
        
        case ast.Deref(loc, left, right) => 
          notImpl(expr)
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case BuiltIn(BuiltIns.Load.name, arity) =>
              emit(actuals.head).map(_ :+ LoadLocal(Het))

            case BuiltIn(n, arity) =>
              notImpl(expr)

            case UserDef(e) =>
              notImpl(expr)

            case NullBinding => 
              notImpl(expr)
          }
        
        case ast.Operation(loc, left, op, right) => 
          notImpl(expr)
        
        case ast.Add(loc, left, right) => 
          emitBinary(left, right, Add)
        
        case ast.Sub(loc, left, right) => 
          emitBinary(left, right, Sub)

        case ast.Mul(loc, left, right) => 
          emitBinary(left, right, Mul)
        
        case ast.Div(loc, left, right) => 
          emitBinary(left, right, Div)
        
        case ast.Lt(loc, left, right) => 
          notImpl(expr)
        
        case ast.LtEq(loc, left, right) => 
          notImpl(expr)
        
        case ast.Gt(loc, left, right) => 
          notImpl(expr)
        
        case ast.GtEq(loc, left, right) => 
          notImpl(expr)
        
        case ast.Eq(loc, left, right) => 
          notImpl(expr)
        
        case ast.NotEq(loc, left, right) => 
          notImpl(expr)
        
        case ast.Or(loc, left, right) => 
          notImpl(expr)
        
        case ast.And(loc, left, right) =>
          notImpl(expr)
        
        case ast.Comp(loc, child) =>
          notImpl(expr)
        
        case ast.Neg(loc, child) => 
          notImpl(expr)
        
        case ast.Paren(loc, child) => 
          notImpl(expr)
      }): EmitterType).map[Vector[Instruction]](vector ++ _)
    }

    emit0(expr, Vector.empty)
  }
}