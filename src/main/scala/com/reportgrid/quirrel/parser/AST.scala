package com.reportgrid.quirrel
package parser

import com.reportgrid.quirrel.util.Atom
import edu.uwm.cs.gll.ast._

trait AST { 
  import Atom._
  
  def prettyPrint(e: Expr, level: Int = 0): String = {
    val indent = 0 until level map Function.const(' ') mkString
    
    e match {
      case Binding(id, params, left, right) => {
        val paramStr = params map { indent + "  - " + _ } mkString "\n"
        
        indent + "type: bind\n" +
          indent + "params:\n" + paramStr + "\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case New(child) => {
        indent + "type: new\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Relate(from: Expr, to: Expr, in: Expr) => {
        indent + "type: relate\n" +
          indent + "from:\n" + prettyPrint(from, level + 2) + "\n" +
          indent + "to:\n" + prettyPrint(to, level + 2) + "\n" +
          indent + "in:\n" + prettyPrint(in, level + 2)
      }
      
      case Var(id) => {
        indent + "type: var\n" +
          indent + "id: " + id
      }
      
      case TicVar(id) => {
        indent + "type: ticvar\n" +
          indent + "id: " + id
      }
      
      case StrLit(value) => {
        indent + "type: str\n" +
          indent + "value: " + value
      }
      
      case NumLit(value) => {
        indent + "type: num\n" +
          indent + "value: " + value
      }
      
      case BoolLit(value) => {
        indent + "type: bool\n" +
          indent + "value: " + value
      }
      
      case ObjectDef(props) => {
        val propStr = props map {
          case (name, value) => {
            indent + "  - \n" +
              indent + "    name: " + name + "\n" +
              indent + "    value:\n" + prettyPrint(value, level + 6)
          }
        } mkString "\n"
        
        indent + "type: object\n" +
          indent + "properties:\n" + propStr
      }
      
      case ArrayDef(values) => {
        val valStr = values map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: array\n" +
          indent + "values:\n" + valStr
      }
      
      case Descent(child, property) => {
        indent + "type: descent\n" +
          indent + "child:\n" + prettyPrint(child, level + 2) + "\n" +
          indent + "property: " + property
      }
      
      case Deref(left, right) => {
        indent + "type: deref\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n"
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Dispatch(name, actuals) => {
        val actualsStr = actuals map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: dispatch\n" +
          indent + "name: " + name + "\n" +
          indent + "actuals:\n" + actualsStr
      }
      
      case Operation(left, op, right) => {
        indent + "type: op\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "op: " + op + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Add(left, right) => {
        indent + "type: add\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Sub(left, right) => {
        indent + "type: sub\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Mul(left, right) => {
        indent + "type: mul\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Div(left, right) => {
        indent + "type: div\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Lt(left, right) => {
        indent + "type: lt\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case LtEq(left, right) => {
        indent + "type: LtEq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Gt(left, right) => {
        indent + "type: gt\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case GtEq(left, right) => {
        indent + "type: GtEq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Eq(left, right) => {
        indent + "type: eq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case NotEq(left, right) => {
        indent + "type: noteq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Or(left, right) => {
        indent + "type: or\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case And(left, right) => {
        indent + "type: and\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Comp(child) => {
        indent + "type: comp\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Neg(child) => {
        indent + "type: neg\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Paren(child) => {
        indent + "type: paren\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
    }
  }
  
  sealed trait Expr extends Node with Product {
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
