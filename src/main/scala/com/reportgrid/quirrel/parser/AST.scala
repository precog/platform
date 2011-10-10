package com.reportgrid.quirrel
package parser

trait AST { outer => 
  def root: Expr
  
  sealed trait Expr {
    def root = outer.root
  }
  
  case class Binding(id: String, params: Vector[String], body: Expr, in: Expr) extends Expr
  
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
  
  case class Dispatch(name: String, actuals: Vector[Expr]) extends Expr
  case class Operation(left: Expr, op: String, right: Expr) extends Expr
  
  case class Add(left: Expr, right: Expr) extends Expr
  case class Sub(left: Expr, right: Expr) extends Expr
  case class Mul(left: Expr, right: Expr) extends Expr
  case class Div(left: Expr, right: Expr) extends Expr
  
  case class Lt(left: Expr, right: Expr) extends Expr
  case class LtEq(left: Expr, right: Expr) extends Expr
  case class Gt(left: Expr, right: Expr) extends Expr
  case class GtEq(left: Expr, right: Expr) extends Expr
  
  case class Eq(left: Expr, right: Expr) extends Expr
  case class NotEq(left: Expr, right: Expr) extends Expr
  
  case class And(left: Expr, right: Expr) extends Expr
  case class Or(left: Expr, right: Expr) extends Expr
  
  case class Comp(body: Expr) extends Expr
  case class Neg(body: Expr) extends Expr
  
  case class Paren(body: Expr) extends Expr
}
