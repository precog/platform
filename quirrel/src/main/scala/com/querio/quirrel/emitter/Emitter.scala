package com.querio.quirrel.emitter

import com.querio.quirrel.parser.AST
import com.querio.bytecode.{Instructions}

trait Emitter extends AST with Instructions {
  import instructions._

  def emit(expr: Expr): Vector[Instruction] = {
    expr match {
      case ast.New(loc, child) => 
      
      case ast.Relate(loc, from: Expr, to: Expr, in: Expr) => 
      
      case t @ ast.TicVar(loc, id) => 
      
      case ast.StrLit(loc, value) => 
      
      case ast.NumLit(loc, value) => 
      
      case ast.BoolLit(loc, value) => 
      
      case ast.ObjectDef(loc, props) => 
      
      case ast.ArrayDef(loc, values) => 
      
      case ast.Descent(loc, child, property) => 
      
      case ast.Deref(loc, left, right) => 
      
      case d @ ast.Dispatch(loc, name, actuals) => 
      
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
    }

    null
  }
}