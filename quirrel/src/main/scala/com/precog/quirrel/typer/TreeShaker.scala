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

trait TreeShaker extends Phases with parser.AST with Binder with Errors {
  import ast._
  import library._

  /**
   * @return The <em>root</em> of the shaken tree
   */
  def shakeTree(tree: Expr): Expr = {
    // if binder rejects a tree, shaking it can remove the errors
    if (bindNames(tree.root) forall isWarning) {
      val (root, _, _, errors) = performShake(tree.root)
      bindRoot(root, root)
      
      root._errors appendFrom tree._errors
      root._errors ++= errors
      
      root
    } else {
      tree.root
    }
  }
  
  def performShake(tree: Expr): (Expr, Set[(Identifier, NameBinding)], Set[(TicId, VarBinding)], Set[Error]) = tree match {
    case b @ Let(loc, id, params, left, right) => {
      lazy val (left2, leftNameBindings, leftVarBindings, leftErrors) = performShake(left)
      val (right2, rightNameBindings, rightVarBindings, rightErrors) = performShake(right)
      
      if (rightNameBindings contains (id -> LetBinding(b))) {
        val ids = params map { Identifier(Vector(), _) }
        val unusedParamBindings = Set(ids zip (Stream continually (FormalBinding(b): NameBinding)): _*) &~ leftNameBindings

        val errors = unusedParamBindings map { case (id, _) => Error(b, UnusedFormalBinding(id)) }
        
        (Let(loc, id, params, left2, right2), leftNameBindings ++ rightNameBindings, leftVarBindings ++ rightVarBindings, leftErrors ++ rightErrors ++ errors)
      } else {
        (right2, rightNameBindings, rightVarBindings, rightErrors + Error(b, UnusedLetBinding(id)))
      }
    }

    case b @ Solve(loc, constraints, child) => {
      val mapped = constraints map performShake
      
      val constraints2 = mapped map { _._1 }
      
      val (constNameVector, constVarVector) = mapped map {
        case (_, names, vars, _) => (names, vars)
      } unzip
      
      val constNames = constNameVector reduce { _ ++ _ }
      val constVars = constVarVector reduce { _ ++ _ }
      
      val constErrors = mapped map { _._4 } reduce { _ ++ _ }
      
      val (child2, childNames, childVars, childErrors) = performShake(child)
      
      val unusedBindings = Set(b.vars.toSeq zip (Stream continually (SolveBinding(b): VarBinding)): _*) &~ childVars
      
      val errors = unusedBindings map { case (id, _) => Error(b, UnusedTicVariable(id)) }
      
      (Solve(loc, constraints2, child2), constNames ++ childNames, constVars ++ childVars, constErrors ++ childErrors ++ errors)
    }
    
    case Import(loc, spec, child) => {
      val (child2, names, vars, errors) = performShake(child)
      (Import(loc, spec, child2), names, vars, errors)
    }
    
    case Assert(loc, pred, child) => {
      val (pred2, predNames, predVars, predErrors) = performShake(pred)
      val (child2, childNames, childVars, childErrors) = performShake(child)
      
      (Assert(loc, pred2, child2), predNames ++ childNames, predVars ++ childVars, predErrors ++ childErrors)
    }    

    case Observe(loc, data, samples) => {
      val (data2, dataNames, dataVars, dataErrors) = performShake(data)
      val (samples2, samplesNames, samplesVars, samplesErrors) = performShake(samples)
      
      (Observe(loc, data2, samples2), dataNames ++ samplesNames, dataVars ++ samplesVars, dataErrors ++ samplesErrors)
    }
    
    case New(loc, child) => {
      val (child2, names, vars, errors) = performShake(child)
      (New(loc, child2), names, vars, errors)
    }
    
    case Relate(loc, from, to, in) => {
      val (from2, fromNames, fromVars, fromErrors) = performShake(from)
      val (to2, toNames, toVars, toErrors) = performShake(to)
      val (in2, inNames, inVars, inErrors) = performShake(in)
      (Relate(loc, from2, to2, in2), fromNames ++ toNames ++ inNames, fromVars ++ toVars ++ inVars, fromErrors ++ toErrors ++ inErrors)
    }
    
    case e @ TicVar(loc, id) =>
      (TicVar(loc, id), Set(), Set(id -> e.binding), Set())
    
    case e @ StrLit(_, _) => (e, Set(), Set(), Set())
    case e @ NumLit(_, _) => (e, Set(), Set(), Set())
    case e @ BoolLit(_, _) => (e, Set(), Set(), Set())
    case e @ UndefinedLit(_) => (e, Set(), Set(), Set())
    case e @ NullLit(_) => (e, Set(), Set(), Set())
    
    case ObjectDef(loc, props) => {
      val mapped: Vector[(String, (Expr, Set[(Identifier, NameBinding)], Set[(TicId, VarBinding)], Set[Error]))] = props map {
        case (key, value) => (key, performShake(value))
      }
      
      val props2 = mapped map { case (key, (value, _, _, _)) => (key, value) }
      
      val (names, vars) = mapped.foldLeft((Set[(Identifier, NameBinding)](), Set[(TicId, VarBinding)]())) {
        case ((names, vars), (_, (_, names2, vars2, _))) => (names ++ names2, vars ++ vars2)
      }
      
      val errors = mapped.foldLeft(Set[Error]()) {
        case (errors, (_, (_, _, _, errors2))) => errors ++ errors2
      }
      
      (ObjectDef(loc, props2), names, vars, errors)
    }
    
    case ArrayDef(loc, values) => {
      val mapped = values map performShake
      val values2 = mapped map { case (value, _, _, _) => value }
      
      val (names, vars) = mapped.foldLeft((Set[(Identifier, NameBinding)](), Set[(TicId, VarBinding)]())) {
        case ((names, vars), (_, names2, vars2, _)) => (names ++ names2, vars ++ vars2)
      }
      
      val errors = mapped.foldLeft(Set[Error]()) {
        case (errors, (_, _, _, errors2)) => errors ++ errors2
      }
      
      (ArrayDef(loc, values2), names, vars, errors)
    }
    
    case Descent(loc, child, property) => {
      val (child2, names, vars, errors) = performShake(child)
      (Descent(loc, child2, property), names, vars, errors)
    }
    
    case MetaDescent(loc, child, property) => {
      val (child2, bindings, vars, errors) = performShake(child)
      (MetaDescent(loc, child2, property), bindings, vars, errors)
    }
    
    case Deref(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Deref(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case d @ Dispatch(loc, name, actuals) => {
      val mapped = actuals map performShake
      val actuals2 = mapped map { case (value, _, _, _) => value }
      
      val (names, vars) = mapped.foldLeft((Set[(Identifier, NameBinding)](), Set[(TicId, VarBinding)]())) {
        case ((names, vars), (_, names2, vars2, _)) => (names ++ names2, vars ++ vars2)
      }
      
      val errors = mapped.foldLeft(Set[Error]()) {
        case (errors, (_, _, _, errors2)) => errors ++ errors2
      }
      
      (Dispatch(loc, name, actuals2), names + (name -> d.binding), vars, errors)
    }

    case Cond(loc, pred, left, right) => {
      val (pred2, predNames, predVars, predErrors) = performShake(pred)
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)

      (Cond(loc, pred2, left2, right2), predNames ++ leftNames ++ rightNames, predVars ++ leftVars ++ rightVars, predErrors ++ leftErrors ++ rightErrors)
    }
    
    case Where(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Where(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case With(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (With(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Intersect(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Intersect(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Union(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Union(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }    

    case Difference(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Difference(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Add(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Add(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Sub(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Sub(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Mul(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Mul(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Div(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Div(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Mod(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Mod(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Pow(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)

      (Pow(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }

    case Lt(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Lt(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case LtEq(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (LtEq(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Gt(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Gt(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case GtEq(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (GtEq(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Eq(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Eq(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case NotEq(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (NotEq(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case And(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (And(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Or(loc, left, right) => {
      val (left2, leftNames, leftVars, leftErrors) = performShake(left)
      val (right2, rightNames, rightVars, rightErrors) = performShake(right)
      
      (Or(loc, left2, right2), leftNames ++ rightNames, leftVars ++ rightVars, leftErrors ++ rightErrors)
    }
    
    case Comp(loc, child) => {
      val (child2, names, vars, errors) = performShake(child)
      (Comp(loc, child2), names, vars, errors)
    }
    
    case Neg(loc, child) => {
      val (child2, names, vars, errors) = performShake(child)
      (Neg(loc, child2), names, vars, errors)
    }
    
    case Paren(_, child) => performShake(child)     // nix parentheses on shake
  }
}


