package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.quirrel.typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import com.querio.bytecode.{Instructions}

import scalaz.{Validation, Success, Failure}
import scalaz.Scalaz._

trait Emitter extends AST with Instructions with Binder with ProvenanceChecker {
  import instructions._
  case class EmitterError(expr: Expr, message: String) extends Exception(message)

  private def notImpl(expr: Expr): Validation[EmitterError, Vector[Instruction]] = Failure(EmitterError(expr, "Not implemented"))

  def emit(expr: Expr): Validation[EmitterError, Vector[Instruction]] = {
    def emit0(expr: Expr, vector: Vector[Instruction]): Validation[EmitterError, Vector[Instruction]] = {
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
          (left.provenance, right.provenance) match {
            case (NullProvenance, _) | (_, NullProvenance) => 
              

            case (StaticProvenance(p1), StaticProvenance(p2)) => 
              

            case _ =>
          }

          notImpl(expr)
        
        case ast.Sub(loc, left, right) => 
          notImpl(expr)

        case ast.Mul(loc, left, right) => 
          notImpl(expr)
        
        case ast.Div(loc, left, right) => 
          notImpl(expr)
        
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
      }): Validation[EmitterError, Vector[Instruction]]).map[Vector[Instruction]](vector ++ _)
    }

    emit0(expr, Vector.empty)
  }
}