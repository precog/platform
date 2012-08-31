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
package com.precog.quirrel
package typer

import com.codecommit.gll.LineStream

trait TreeShaker extends Phases with parser.AST with Binder {
  import ast._

  /**
   * @return The <em>root</em> of the shaken tree
   */
  def shakeTree(tree: Expr): Expr = {
    val (root, _, errors) = performShake(tree.root)
    bindRoot(root, root)
    
    root._errors appendFrom tree._errors
    root._errors ++= errors
    
    root
  }
  
  def performShake(tree: Expr): (Expr, Set[(Either[TicId, Identifier], Binding)], Set[Error]) = tree match {
    case b @ Let(loc, id, params, left, right) => {
      lazy val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      if (rightBindings contains (Right(id) -> LetBinding(b))) {
        val unusedParamBindings = Set(params zip (Stream continually (LetBinding(b): Binding)): _*) &~ leftBindings.collect { 
          case (Left(id), b) => (id, b)
        }  

        val errors = unusedParamBindings map { case (id, _) => Error(b, UnusedTicVariable(id)) }
        
        (Let(loc, id, params, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors ++ errors)
      } else {
        (right2, rightBindings, rightErrors + Error(b, UnusedLetBinding(id)))
      }
    }

    case b @ Solve(loc, constraints, child) =>
      sys.error("todo")
    
    case Import(loc, spec, child) => {
      val (child2, bindings, errors) = performShake(child)
      (Import(loc, spec, child2), bindings, errors)
    }
    
    case New(loc, child) => {
      val (child2, bindings, errors) = performShake(child)
      (New(loc, child2), bindings, errors)
    }
    
    case Relate(loc, from, to, in) => {
      val (from2, fromBindings, fromErrors) = performShake(from)
      val (to2, toBindings, toErrors) = performShake(to)
      val (in2, inBindings, inErrors) = performShake(in)
      (Relate(loc, from2, to2, in2), fromBindings ++ toBindings ++ inBindings, fromErrors ++ toErrors ++ inErrors)
    }
    
    case e @ TicVar(loc, id) =>
      (TicVar(loc, id), Set(Left(id) -> e.binding), Set())
    
    case e @ StrLit(_, _) => (e, Set(), Set())
    case e @ NumLit(_, _) => (e, Set(), Set())
    case e @ BoolLit(_, _) => (e, Set(), Set())
    case e @ NullLit(_) => (e, Set(), Set())
    
    case ObjectDef(loc, props) => {
      val mapped = props map {
        case (key, value) => (key, performShake(value))
      }
      
      val props2 = mapped map { case (key, (value, _, _)) => (key, value) }
      
      val bindings = mapped.foldLeft(Set[(Either[TicId, Identifier], Binding)]()) {
        case (bindings, (_, (_, bindings2, _))) => bindings ++ bindings2
      }
      
      val errors = mapped.foldLeft(Set[Error]()) {
        case (errors, (_, (_, _, errors2))) => errors ++ errors2
      }
      
      (ObjectDef(loc, props2), bindings, errors)
    }
    
    case ArrayDef(loc, values) => {
      val mapped = values map performShake
      val values2 = mapped map { case (value, _, _) => value }
      
      val bindings = mapped.foldLeft(Set[(Either[TicId, Identifier], Binding)]()) {
        case (bindings, (_, bindings2, _)) => bindings ++ bindings2
      }
      
      val errors = mapped.foldLeft(Set[Error]()) {
        case (errors, (_, _, errors2)) => errors ++ errors2
      }
      
      (ArrayDef(loc, values2), bindings, errors)
    }
    
    case Descent(loc, child, property) => {
      val (child2, bindings, errors) = performShake(child)
      (Descent(loc, child2, property), bindings, errors)
    }
    
    case Deref(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Deref(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case d @ Dispatch(loc, name, actuals) => {
      val mapped = actuals map performShake
      val actuals2 = mapped map { case (value, _, _) => value }
      
      val bindings = mapped.foldLeft(Set[(Either[TicId, Identifier], Binding)]()) {
        case (bindings, (_, bindings2, _)) => bindings ++ bindings2
      }
      
      val errors = mapped.foldLeft(Set[Error]()) {
        case (errors, (_, _, errors2)) => errors ++ errors2
      }
      
      (Dispatch(loc, name, actuals2), bindings + (Right(name) -> d.binding), errors)
    }
    
    case Where(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Where(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case With(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (With(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Intersect(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Intersect(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Union(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Union(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }    

    case Difference(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Difference(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Add(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Add(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Sub(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Sub(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Mul(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Mul(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Div(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Div(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Lt(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Lt(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case LtEq(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (LtEq(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Gt(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Gt(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case GtEq(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (GtEq(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Eq(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Eq(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case NotEq(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (NotEq(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case And(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (And(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Or(loc, left, right) => {
      val (left2, leftBindings, leftErrors) = performShake(left)
      val (right2, rightBindings, rightErrors) = performShake(right)
      
      (Or(loc, left2, right2), leftBindings ++ rightBindings, leftErrors ++ rightErrors)
    }
    
    case Comp(loc, child) => {
      val (child2, bindings, errors) = performShake(child)
      (Comp(loc, child2), bindings, errors)
    }
    
    case Neg(loc, child) => {
      val (child2, bindings, errors) = performShake(child)
      (Neg(loc, child2), bindings, errors)
    }
    
    case Paren(_, child) => performShake(child)     // nix parentheses on shake
  }
}


