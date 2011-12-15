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