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
package com.reportgrid.quirrel
package parser

import com.reportgrid.quirrel.util.Atom
import edu.uwm.cs.gll.ast._
import com.reportgrid.quirrel.util.SetAtom

trait AST extends Passes { 
  import Atom._
  
  type Binding
  
  def prettyPrint(e: Expr, level: Int = 0): String = {
    val indent = 0 until level map Function.const(' ') mkString
    
    val back = e match {
      case Let(id, params, left, right) => {
        val paramStr = params map { indent + "  - " + _ } mkString "\n"
        
          indent + "type: let\n" +
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
      
      case d @ Dispatch(name, actuals) => {
        val actualsStr = actuals map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: dispatch\n" +
          indent + "name: " + name + "\n" +
          indent + "actuals:\n" + actualsStr + "\n" +
          indent + "binding: " + d.binding.toString
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
    
    indent + "nodeId: " + e.nodeId + "\n" + back
  }
  
  sealed trait Expr extends Node with Product {
    val nodeId = System.identityHashCode(this)
    
    private[parser] val _root = atom[Expr]
    
    def root = _root()
    
    protected final lazy val _errors: SetAtom[Error] =
      if (this eq root) new SetAtom[Error] else root._errors
    
    final def errors= _errors()
  }
  
  case class Let(id: String, params: Vector[String], left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'let
  }
  
  case class New(child: Expr) extends Expr with UnaryNode {
    val label = 'new
  }
  
  case class Relate(from: Expr, to: Expr, in: Expr) extends Expr {
    val label = 'relate
    
    def children = List(from, to, in)
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
    
    private[quirrel] val _binding = atom[Binding] {
      _errors ++= bindNames(root)
    }
    
    def binding = _binding()
    
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
