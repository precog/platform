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
import edu.uwm.cs.gll.LineStream
import edu.uwm.cs.gll.ast._

trait AST { 
  import Atom._
  
  def prettyPrint(e: Expr, level: Int = 0): String = {
    val indent = 0 until level map Function.const(' ') mkString
    
    val back = e match {
      case Binding(loc, id, params, left, right) => {
        val paramStr = params map { indent + "  - " + _ } mkString "\n"
        
        indent + "type: bind\n" +
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
      
      case Var(loc, id) => {
        indent + "type: var\n" +
          indent + "id: " + id
      }
      
      case TicVar(loc, id) => {
        indent + "type: ticvar\n" +
          indent + "id: " + id
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
      
      case Dispatch(loc, name, actuals) => {
        val actualsStr = actuals map { indent + "  -\n" + prettyPrint(_, level + 4) } mkString "\n"
        
        indent + "type: dispatch\n" +
          indent + "name: " + name + "\n" +
          indent + "actuals:\n" + actualsStr
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
    
    indent + "line: " + e.loc.lineNum + "\n" +
      indent + "col: " + e.loc.colNum + "\n" +
      back
  }
  
  sealed trait Expr extends Node with Product {
    private[parser] val _root = atom[Expr]
    
    def root = _root()
    
    def loc: LineStream
  }
  
  case class Binding(loc: LineStream, id: String, params: Vector[String], left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'bind
  }
  
  case class New(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'new
  }
  
  case class Relate(loc: LineStream, from: Expr, to: Expr, in: Expr) extends Expr {
    val label = 'relate
    
    def children = List(from, to, in)
  }
  
  case class Var(loc: LineStream, id: String) extends Expr with LeafNode {
    val label = 'var
  }
  
  case class TicVar(loc: LineStream, id: String) extends Expr with LeafNode {
    val label = 'ticvar
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
  }
  
  case class Deref(loc: LineStream, left: Expr, right: Expr) extends Expr with BinaryNode {
    val label = 'deref
  }
  
  case class Dispatch(loc: LineStream, name: String, actuals: Vector[Expr]) extends Expr {
    val label = 'dispatch
    
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
  }
  
  case class Neg(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'neg
  }
  
  case class Paren(loc: LineStream, child: Expr) extends Expr with UnaryNode {
    val label = 'paren
  }
}
