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

import scalaz.{StateT, Id, Identity, Bind, Monoid}
import scalaz.Scalaz._

trait Emitter extends AST with Instructions with Binder with ProvenanceChecker {
  import instructions._
  case class EmitterError(expr: Expr, message: String) extends Exception(message)

  private def nullProvenanceError[A](expr: Expr): A = throw EmitterError(expr, "Expression has null provenance")
  private def notImpl[A](expr: Expr): A = throw EmitterError(expr, "Not implemented")

  private case class Emission(
    bytecode: Vector[Instruction] = Vector.empty
  )

  // This doesn't seem to work
  /*private implicit def BindSemigroup[M[_], A](implicit bind: Bind[M]): Semigroup[M[A]] = new Semigroup[M[A]] {
    def append(v1: M[A], v2: => M[A]): M[A] = bind.bind(v1)((a: A) => v2)
  }*/
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private implicit val EmitterStateMonoid: Monoid[EmitterState] = new Monoid[EmitterState] {
    val zero = StateT.stateT[Id, Unit, Emission](())

    def append(v1: EmitterState, v2: => EmitterState): EmitterState = v1 >> v2
  }

  private object Emission {
    def emitInstr(i: Instruction): EmitterState = StateT.apply[Id, Emission, Unit](e => (Unit, e.copy(bytecode = e.bytecode :+ i)))

    def emitInstrs(is: Seq[Instruction]): EmitterState = StateT.apply[Id, Emission, Unit](e => (Unit, e.copy(bytecode = e.bytecode ++ is)))
  }

  def emit(expr: Expr): Vector[Instruction] = {
    import Emission._

    def emitMap(left: Expr, right: Expr, op: BinaryOperation): EmitterState = {
      val mapInstr = emitInstr((left.provenance, right.provenance) match {
        case (NullProvenance, _) =>
          nullProvenanceError(left)

        case (_, NullProvenance) =>
          nullProvenanceError(right)

        case (StaticProvenance(p1), StaticProvenance(p2)) if (p1 == p2) => 
          Map2Match(op)
        
        case (DynamicProvenance(id1), DynamicProvenance(id2)) if (id1 == id2) =>
          Map2Match(op)

        case (ValueProvenance, p2) if (p2 != ValueProvenance) =>
          Map2CrossRight(op)

        case (p1, ValueProvenance) if (p1 != ValueProvenance) =>
          Map2CrossLeft(op)

        case (_, _) =>
          Map2Cross(op)
      })

      emitExpr(left) >> emitExpr(right) >> mapInstr
    }

    def emitFilter(left: Expr, right: Expr, depth: Short = 0, pred: Option[Predicate] = None): EmitterState = {
      val filterInstr = emitInstr((left.provenance, right.provenance) match {
        case (NullProvenance, _) =>
          nullProvenanceError(left)

        case (_, NullProvenance) =>
          nullProvenanceError(right)

        case (StaticProvenance(p1), StaticProvenance(p2)) if (p1 == p2) => 
          FilterMatch(depth, pred)
        
        case (DynamicProvenance(id1), DynamicProvenance(id2)) if (id1 == id2) =>
          FilterMatch(depth, pred)

        case (_, _) =>
          FilterCross(depth, pred)
      })

      emitExpr(left) >> emitExpr(right) >> filterInstr
    }

    def emitExpr(expr: Expr): StateT[Id, Emission, Unit] = {
      expr match {
        case ast.Let(loc, id, params, left, right) =>
          params.length match {
            case 0 =>
              emitExpr(right)

            case n =>
              notImpl(expr)
          }

        case ast.New(loc, child) => 
          notImpl(expr)
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          notImpl(expr)
        
        case t @ ast.TicVar(loc, id) => 
          notImpl(expr)
        
        case ast.StrLit(loc, value) => 
          emitInstr(PushString(value))
        
        case ast.NumLit(loc, value) => 
          emitInstr(PushNum(value))
        
        case ast.BoolLit(loc, value) => 
          emitInstr(value match {
            case true  => PushTrue
            case false => PushFalse
          })
        
        case ast.ObjectDef(loc, props) => 
          def field2ObjInstr(t: (String, Expr)) = emitInstr(PushString(t._1)) >> emitExpr(t._2) >> emitInstr(Map2Cross(WrapObject))

          // TODO: Non-constants
          val singles = props.map(field2ObjInstr)
          val joins   = Vector.fill(props.length - 1)(emitInstr(Map2Cross(JoinObject)))

          (singles ++ joins).foldLeft(mzero[EmitterState])(_ |+| _)
        
        case ast.ArrayDef(loc, values) => 
          // TODO: Non-constants
          val singles = values.map(v => emitExpr(v) >> emitInstr(Map1(WrapArray)))
          val joins   = Vector.fill(values.length - 1)(emitInstr(Map2Cross(JoinArray)))

          (singles ++ joins).foldLeft(mzero[EmitterState])(_ |+| _)
        
        case ast.Descent(loc, child, property) => 
          // Object
          // TODO: Why is "property" not expression???????
          emitExpr(child) >> emitInstr(PushString(property)) >> emitInstr(Map2Cross(DerefObject))
        
        case ast.Deref(loc, left, right) => 
          // TODO: Non-constants for 'right'
          emitExpr(left) >> emitExpr(right) >> emitInstr(Map2Cross(DerefArray))
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case BuiltIn(BuiltIns.Load.name, arity) =>
              assert(arity == 1)

              emitExpr(actuals.head) >> emitInstr(LoadLocal(Het))

            case BuiltIn(n, arity) =>
              notImpl(expr)

            case UserDef(ast.Let(loc, id, params, left, right)) =>
              emitExpr(left)

            case NullBinding => 
              notImpl(expr)
          }
        
        case ast.Operation(loc, left, op, right) => 
          // WHERE clause -- to be refactored (!)
          op match {
            case "where" => 
              emitFilter(left, right, 0, None)

            case _ => notImpl(expr)
          }
        
        case ast.Add(loc, left, right) => 
          emitMap(left, right, Add)
        
        case ast.Sub(loc, left, right) => 
          emitMap(left, right, Sub)

        case ast.Mul(loc, left, right) => 
          emitMap(left, right, Mul)
        
        case ast.Div(loc, left, right) => 
          emitMap(left, right, Div)
        
        case ast.Lt(loc, left, right) => 
          emitMap(left, right, Lt)
        
        case ast.LtEq(loc, left, right) => 
          emitMap(left, right, LtEq)
        
        case ast.Gt(loc, left, right) => 
          emitMap(left, right, Gt)
        
        case ast.GtEq(loc, left, right) => 
          emitMap(left, right, GtEq)
        
        case ast.Eq(loc, left, right) => 
          emitMap(left, right, Eq)
        
        case ast.NotEq(loc, left, right) => 
          emitMap(left, right, NotEq)
        
        case ast.Or(loc, left, right) => 
          emitMap(left, right, Or)
        
        case ast.And(loc, left, right) =>
          emitMap(left, right, And)
        
        case ast.Comp(loc, child) =>
          emitExpr(child) >> emitInstr(Map1(Comp))
        
        case ast.Neg(loc, child) => 
          emitExpr(child) >> emitInstr(Map1(Neg))
        
        case ast.Paren(loc, child) => 
          StateT.stateT[Id, Unit, Emission](Unit)
      }
    }

    emitExpr(expr).exec(Emission()).bytecode
  }
}