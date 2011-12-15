package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.quirrel.typer.Binder
import com.querio.bytecode.{Instructions}

import scalaz._

trait Emitter extends AST with Instructions with Binder {
  import instructions._

  def emit(expr: Expr): Vector[Instruction] = {
    def emit0(expr: Expr, vector: Vector[Instruction]): Vector[Instruction] = {
      vector ++ (expr match {
        case ast.New(loc, child) => 
          Vector.empty
        
        case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
          Vector.empty
        
        case t @ ast.TicVar(loc, id) => 
          Vector.empty
        
        case ast.StrLit(loc, value) => 
          Vector(PushString(value))
        
        case ast.NumLit(loc, value) => 
          Vector(PushNum(value))
        
        case ast.BoolLit(loc, value) => 
          value match {
            case true  => Vector(PushTrue)
            case false => Vector(PushFalse)
          }
        
        case ast.ObjectDef(loc, props) => 
          Vector.empty
        
        case ast.ArrayDef(loc, values) => 
          Vector.empty
        
        case ast.Descent(loc, child, property) => 
          Vector.empty
        
        case ast.Deref(loc, left, right) => 
          Vector.empty
        
        case d @ ast.Dispatch(loc, name, actuals) => 
          d.binding match {
            case BuiltIn(BuiltIns.Load.name, arity) =>
              emit(actuals.head) :+ LoadLocal(Het)

            case BuiltIn(n, arity) =>
              Vector.empty

            case UserDef(e) =>
              Vector.empty

            case NullBinding => 
              Vector.empty
          }
        
        case ast.Operation(loc, left, op, right) => 
          Vector.empty
        
        case ast.Add(loc, left, right) => 
          Vector.empty
        
        case ast.Sub(loc, left, right) => 
          Vector.empty

        case ast.Mul(loc, left, right) => 
          Vector.empty
        
        case ast.Div(loc, left, right) => 
          Vector.empty
        
        case ast.Lt(loc, left, right) => 
          Vector.empty
        
        case ast.LtEq(loc, left, right) => 
          Vector.empty
        
        case ast.Gt(loc, left, right) => 
          Vector.empty
        
        case ast.GtEq(loc, left, right) => 
          Vector.empty
        
        case ast.Eq(loc, left, right) => 
          Vector.empty
        
        case ast.NotEq(loc, left, right) => 
          Vector.empty
        
        case ast.Or(loc, left, right) => 
          Vector.empty
        
        case ast.And(loc, left, right) =>
          Vector.empty
        
        case ast.Comp(loc, child) =>
          Vector.empty
        
        case ast.Neg(loc, child) => 
          Vector.empty
        
        case ast.Paren(loc, child) => 
          Vector.empty
      })
    }

    emit0(expr, Vector.empty)
  }
}