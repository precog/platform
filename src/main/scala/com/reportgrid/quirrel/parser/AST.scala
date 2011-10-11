package com.reportgrid.quirrel
package parser

import com.reportgrid.quirrel.util.Atom
import edu.uwm.cs.gll.ast._

trait AST { 
  import Atom._
  
  sealed trait Expr extends Node {
    private[parser] val _root = atom[Expr]
    
    def root = _root()
  }
  
  case class Binding(id: String, params: Vector[String], left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'bind
  }
  
  case class New(child: Expr) extends Expr with UnaryNode {
    val label = 'new
  }
  
  case class Relate(from: Expr, to: Expr, in: Expr) extends Expr {
    val label = 'relate
    
    def children = List(from, to, in)
  }
  
  case class Var(id: String) extends Expr with LeafNode {
    val label = 'var
  }
  
  case class TicVar(id: String) extends Expr with LeafNode {
    val label = 'ticvar
  }
  
  case class StrLit(value: String) extends Expr with LeafNode {
    val label = 'str
  }
  
  case class NumLit(value: String) extends Expr with LeafNode {
    val label = 'num
  }
  
  case class BoolLit(value: Boolean) extends Expr with LeafNode {
    val label = 'bool
  }
  
  case class ObjectDef(props: Vector[(String, Expr)]) extends Expr {
    val label = 'object
    
    def children = props map { _._2 } toList
  }
  
  case class ArrayDef(values: Vector[Expr]) extends Expr {
    val label = 'array
    
    def children = values.toList
  }
  
  case class Descent(child: Expr, property: String) extends Expr with UnaryNode {
    val label = 'descent
  }
  
  case class Deref(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'deref
  }
  
  case class Dispatch(name: String, actuals: Vector[Expr]) extends Expr {
    val label = 'dispatch
    
    def children = actuals.toList
  }
  
  case class Operation(left: Expr, op: String, right: Expr) extends Expr with BinaryNode {
    val label = if (op == "where") 'where else 'op
  }
  
  case class Add(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'add
  }
  
  case class Sub(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'sub
  }
  
  case class Mul(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'mul
  }
  
  case class Div(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'div
  }
  
  case class Lt(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'lt
  }
  
  case class LtEq(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'lteq
  }
  
  case class Gt(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'gt
  }
  
  case class GtEq(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'gteq
  }
  
  case class Eq(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'eq
  }
  
  case class NotEq(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'noteq
  }
  
  case class And(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'and
  }
  
  case class Or(left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'or
  }
  
  case class Comp(child: Expr) extends Expr with UnaryNode {
    val label = 'comp
  }
  
  case class Neg(child: Expr) extends Expr with UnaryNode {
    val label = 'neg
  }
  
  case class Paren(child: Expr) extends Expr with UnaryNode {
    val label = 'paren
  }
}
