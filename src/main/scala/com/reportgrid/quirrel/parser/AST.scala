package com.reportgrid.quirrel
package parser

import com.reportgrid.quirrel.util.{Atom, SetAtom}
import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.ast._

trait AST extends Phases {
  import Atom._
  
  type Binding
  type FormalBinding
  type Provenance
  
  def prettyPrint(e: Expr, level: Int = 0): String = {
    val indent = 0 until level map Function.const(' ') mkString
    
    val back = e match {
      case Let(loc, id, params, left, right) => {
        val paramStr = params map { indent + "  - " + _ } mkString "\n"
        
        indent + "type: let\n" +
          indent + "id: " + id + "\n" +
          indent + "params:\n" + paramStr + "\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case New(loc, child) => {
        indent + "type: new\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Relate(loc, from: Expr, to: Expr, in: Expr) => {
        indent + "type: relate\n" +
          indent + "from:\n" + prettyPrint(from, level + 2) + "\n" +
          indent + "to:\n" + prettyPrint(to, level + 2) + "\n" +
          indent + "in:\n" + prettyPrint(in, level + 2)
      }
      
      case t @ TicVar(loc, id) => {
        indent + "type: ticvar\n" +
          indent + "id: " + id + "\n" +
          indent + "binding: " + t.binding.toString
      }
      
      case StrLit(loc, value) => {
        indent + "type: str\n" +
          indent + "value: " + value
      }
      
      case NumLit(loc, value) => {
        indent + "type: num\n" +
          indent + "value: " + value
      }
      
      case BoolLit(loc, value) => {
        indent + "type: bool\n" +
          indent + "value: " + value
      }
      
      case ObjectDef(loc, props) => {
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
      
      case ArrayDef(loc, values) => {
        val valStr = values map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: array\n" +
          indent + "values:\n" + valStr
      }
      
      case Descent(loc, child, property) => {
        indent + "type: descent\n" +
          indent + "child:\n" + prettyPrint(child, level + 2) + "\n" +
          indent + "property: " + property
      }
      
      case Deref(loc, left, right) => {
        indent + "type: deref\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n"
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case d @ Dispatch(loc, name, actuals) => {
        val actualsStr = actuals map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: dispatch\n" +
          indent + "name: " + name + "\n" +
          indent + "actuals:\n" + actualsStr + "\n" +
          indent + "binding: " + d.binding.toString
      }
      
      case Operation(loc, left, op, right) => {
        indent + "type: op\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "op: " + op + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Add(loc, left, right) => {
        indent + "type: add\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Sub(loc, left, right) => {
        indent + "type: sub\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Mul(loc, left, right) => {
        indent + "type: mul\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Div(loc, left, right) => {
        indent + "type: div\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Lt(loc, left, right) => {
        indent + "type: lt\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case LtEq(loc, left, right) => {
        indent + "type: LtEq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Gt(loc, left, right) => {
        indent + "type: gt\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case GtEq(loc, left, right) => {
        indent + "type: GtEq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Eq(loc, left, right) => {
        indent + "type: eq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case NotEq(loc, left, right) => {
        indent + "type: noteq\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Or(loc, left, right) => {
        indent + "type: or\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case And(loc, left, right) => {
        indent + "type: and\n" +
          indent + "left:\n" + prettyPrint(left, level + 2) + "\n" +
          indent + "right:\n" + prettyPrint(right, level + 2)
      }
      
      case Comp(loc, child) => {
        indent + "type: comp\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Neg(loc, child) => {
        indent + "type: neg\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
      
      case Paren(loc, child) => {
        indent + "type: paren\n" +
          indent + "child:\n" + prettyPrint(child, level + 2)
      }
    }
    
    indent + "nodeId: " + e.nodeId + "\n" +
      indent + "line: " + e.loc.lineNum + "\n" +
      indent + "col: " + e.loc.colNum + "\n" +
      back + "\n" +
      indent + "provenance: " + e.provenance.toString
  }
  
  sealed trait Expr extends Node with Product {
    val nodeId = System.identityHashCode(this)
    
    private[parser] val _root = atom[Expr]
    
    def root = _root()
    
    private[quirrel] val _provenance = atom[Provenance] {
      _errors ++= checkProvenance(root)
    }
    
    def provenance = _provenance()
    
    protected final lazy val _errors: SetAtom[Error] =
      if (this eq root) new SetAtom[Error] else root._errors
    
    final def errors = _errors()
    
    def loc: LineStream
  }
  
  case class Let(loc: LineStream, id: String, params: Vector[String], left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'let
  }
  
  case class New(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'new
    val isPrefix = true
  }
  
  case class Relate(loc: LineStream, from: Expr, to: Expr, in: Expr) extends Expr with BinaryNode {
    val label = 'relate
    
    val left = from
    val right = to
    override def children = List(from, to, in)
  }
  
  case class TicVar(loc: LineStream, id: String) extends Expr with LeafNode {
    val label = 'ticvar
    
    private[quirrel] val _binding = atom[FormalBinding] {
      _errors ++= bindNames(root)
    }
    
    def binding = _binding()
  }
  
  case class StrLit(loc: LineStream, value: String) extends Expr with LeafNode {
    val label = 'str
  }
  
  case class NumLit(loc: LineStream, value: String) extends Expr with LeafNode {
    val label = 'num
  }
  
  case class BoolLit(loc: LineStream, value: Boolean) extends Expr with LeafNode {
    val label = 'bool
  }
  
  case class ObjectDef(loc: LineStream, props: Vector[(String, Expr)]) extends Expr {
    val label = 'object
    
    def children = props map { _._2 } toList
  }
  
  case class ArrayDef(loc: LineStream, values: Vector[Expr]) extends Expr {
    val label = 'array
    
    def children = values.toList
  }
  
  case class Descent(loc: LineStream, child: Expr, property: String) extends Expr with UnaryNode {
    val label = 'descent
    val isPrefix = false
  }
  
  case class Deref(loc: LineStream, left: Expr, right: Expr) extends Expr with UnaryNode {
    val label = 'deref
    val isPrefix = true
    val child = left
  }
  
  case class Dispatch(loc: LineStream, name: String, actuals: Vector[Expr]) extends Expr {
    val label = 'dispatch
    
    private[quirrel] val _binding = atom[Binding] {
      _errors ++= bindNames(root)
    }
    
    def binding = _binding()
    
    def children = actuals.toList
  }
  
  case class Operation(loc: LineStream, left: Expr, op: String, right: Expr) extends Expr with BinaryNode {
    val label = if (op == "where") 'where else 'op
  }
  
  case class Add(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'add
  }
  
  case class Sub(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'sub
  }
  
  case class Mul(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'mul
  }
  
  case class Div(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'div
  }
  
  case class Lt(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'lt
  }
  
  case class LtEq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'lteq
  }
  
  case class Gt(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'gt
  }
  
  case class GtEq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'gteq
  }
  
  case class Eq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'eq
  }
  
  case class NotEq(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'noteq
  }
  
  case class And(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'and
  }
  
  case class Or(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'or
  }
  
  case class Comp(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'comp
    val isPrefix = true
  }
  
  case class Neg(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'neg
    val isPrefix = true
  }
  
  case class Paren(loc: LineStream, child: Expr) extends Expr {
    val label = 'paren
    val children = child :: Nil
  }
}
