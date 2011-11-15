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
package typer

trait TreeShaker extends parser.AST with Binder {
  
  /**
   * @return The <em>root</em> of the shaken tree
   */
  def shakeTree(tree: Expr): Expr = {
    val (root, _) = performShake(tree.root)
    bindRoot(root, root)
    root
  }
  
  private def performShake(tree: Expr): (Expr, Set[Binding]) = tree match {
    case b @ Let(loc, id, params, left, right) => {        // TODO warnings and errors
      lazy val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      if (rightBindings contains UserDef(b))
        (Let(loc, id, params, left2, right2), leftBindings ++ rightBindings)
      else
        (right2, rightBindings)
    }
    
    case New(loc, child) => {
      val (child2, bindings) = performShake(child)
      (New(loc, child2), bindings)
    }
    
    case Relate(loc, from, to, in) => {
      val (from2, fromBindings) = performShake(from)
      val (to2, toBindings) = performShake(to)
      val (in2, inBindings) = performShake(in)
      (Relate(loc, from2, to2, in2), fromBindings ++ toBindings ++ inBindings)
    }
    
    case e @ TicVar(_, _) => (e, Set(e.binding))
    
    case e @ StrLit(_, _) => (e, Set())
    case e @ NumLit(_, _) => (e, Set())
    case e @ BoolLit(_, _) => (e, Set())
    
    case ObjectDef(loc, props) => {
      val mapped = props map {
        case (key, value) => (key, performShake(value))
      }
      
      val props2 = mapped map { case (key, (value, _)) => (key, value) }
      
      val bindings = mapped.foldLeft(Set[Binding]()) {
        case (bindings, (_, (_, bindings2))) => bindings ++ bindings2
      }
      
      (ObjectDef(loc, props2), bindings)
    }
    
    case ArrayDef(loc, values) => {
      val mapped = values map performShake
      val values2 = mapped map { case (value, _) => value }
      
      val bindings = mapped.foldLeft(Set[Binding]()) {
        case (bindings, (_, bindings2)) => bindings ++ bindings2
      }
      
      (ArrayDef(loc, values2), bindings)
    }
    
    case Descent(loc, child, property) => {
      val (child2, bindings) = performShake(child)
      (Descent(loc, child2, property), bindings)
    }
    
    case Deref(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Deref(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case d @ Dispatch(loc, name, actuals) => {
      val mapped = actuals map performShake
      val actuals2 = mapped map { case (value, _) => value }
      
      val bindings = mapped.foldLeft(Set[Binding]()) {
        case (bindings, (_, bindings2)) => bindings ++ bindings2
      }
      
      (Dispatch(loc, name, actuals2), bindings + d.binding)
    }
    
    case Operation(loc, left, op, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Operation(loc, left2, op, right2), leftBindings ++ rightBindings)
    }
    
    case Add(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Add(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Sub(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Sub(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Mul(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Mul(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Div(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Div(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Lt(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Lt(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case LtEq(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (LtEq(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Gt(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Gt(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case GtEq(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (GtEq(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Eq(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Eq(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case NotEq(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (NotEq(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case And(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (And(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Or(loc, left, right) => {
      val (left2, leftBindings) = performShake(left)
      val (right2, rightBindings) = performShake(right)
      
      (Or(loc, left2, right2), leftBindings ++ rightBindings)
    }
    
    case Comp(loc, child) => {
      val (child2, bindings) = performShake(child)
      (Comp(loc, child2), bindings)
    }
    
    case Neg(loc, child) => {
      val (child2, bindings) = performShake(child)
      (Neg(loc, child2), bindings)
    }
    
    case Paren(_, child) => performShake(child)     // nix parentheses on shake
  }
}
