package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.quirrel.typer.{Binder, ProvenanceChecker, CriticalConditionFinder}
import com.querio.bytecode.{Instructions}

import scalaz.{StateT, Id, Identity}
import scalaz.Scalaz._

trait Emitter extends AST with Instructions with Binder with ProvenanceChecker {
  import instructions._
  case class EmitterError(expr: Expr, message: String) extends Exception(message)

  private def nullProvenanceError[A](expr: Expr): A = throw EmitterError(expr, "Expression has null provenance")
  private def notImpl[A](expr: Expr): A = throw EmitterError(expr, "Not implemented")

  private case class Emission(
    bytecode: Vector[Instruction] = Vector.empty
  )
  
  private type EmitterState = StateT[Id, Emission, Unit]

  private object Emission {
    def emitInstr(i: Instruction): EmitterState = StateT.apply[Id, Emission, Unit](e => (Unit, e.copy(bytecode = e.bytecode :+ i)))

    def emitInstrs(is: Seq[Instruction]): EmitterState = StateT.apply[Id, Emission, Unit](e => (Unit, e.copy(bytecode = e.bytecode ++ is)))
  }

  def emit(expr: Expr): Vector[Instruction] = {
    import Emission._

    def emitExprBinary(left: Expr, right: Expr, op: BinaryOperation): EmitterState = {
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
              // TODO: Not correct in general
              Map2Cross(op)
          }

          emitExpr(left) >> emitExpr(right) >> emitInstr(bytecode)
      }
    }

    def emitExpr(expr: Expr): StateT[Id, Emission, _] = {
      ((expr match {
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
              emitExpr(actuals.head) >> emitInstr(LoadLocal(Het))

            case BuiltIn(n, arity) =>
              notImpl(expr)

            case UserDef(ast.Let(loc, id, params, left, right)) =>
              emitExpr(left)

            case NullBinding => 
              notImpl(expr)
          }
        
        case ast.Operation(loc, left, op, right) => 
          notImpl(expr)
        
        case ast.Add(loc, left, right) => 
          emitExprBinary(left, right, Add)
        
        case ast.Sub(loc, left, right) => 
          emitExprBinary(left, right, Sub)

        case ast.Mul(loc, left, right) => 
          emitExprBinary(left, right, Mul)
        
        case ast.Div(loc, left, right) => 
          emitExprBinary(left, right, Div)
        
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
          emitExpr(child) >> emitInstr(Map1(Neg))
        
        case ast.Paren(loc, child) => 
          // NOOP
          StateT.stateT[Id, Unit, Emission](Unit)
      }))
    }

    emitExpr(expr).exec(Emission()).bytecode
  }
}