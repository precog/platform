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

trait CriticalConditionFinder extends parser.AST with Binder {
  import Utils._
  import ast._
  
  override def findCriticalConditions(expr: Expr): Map[TicId, Set[ConditionTree]] = {
    import condition._
    
    def loop(root: Solve, expr: Expr, currentWhere: Option[Expr]): Map[TicId, Set[ConditionTree]] = expr match {
      case Let(_, _, _, left, right) => loop(root, right, currentWhere)
      
      case Import(_, _, child) => loop(root, child, currentWhere)
      
      case New(_, child) => loop(root, child, currentWhere)
      
      case Relate(_, from, to, in) => {
        val first = loop(root, from, currentWhere)
        val second = loop(root, to, currentWhere)
        val third = loop(root, in, currentWhere)
        merge(merge(first, second), third)
      }
      
      case t @ TicVar(_, id) => t.binding match {
        case SolveBinding(`root`) => currentWhere map { where => Map(id -> Set(Condition(where): ConditionTree)) } getOrElse Map()
        case _ => Map()
      }
      
      case StrLit(_, _) => Map()
      case NumLit(_, _) => Map()
      case BoolLit(_, _) => Map()
      case NullLit(_) => Map()
      
      case ObjectDef(_, props) => {
        val maps = props map { case (_, expr) => loop(root, expr, currentWhere) }
        maps.fold(Map())(merge)
      }
      
      case ArrayDef(_, values) => {
        val maps = values map { expr => loop(root, expr, currentWhere) }
        maps.fold(Map())(merge)
      }
      
      case Descent(_, child, _) => loop(root, child, currentWhere)
      
      case MetaDescent(_, child, _) => loop(root, child, currentWhere)
      
      case Deref(loc, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case d @ Dispatch(_, _, actuals) => {
        val maps = actuals map { expr => loop(root, expr, currentWhere) }
        val merged = maps.fold(Map())(merge)
        
        val fromDef = d.binding match {
          case LetBinding(e) => loop(root, e.left, currentWhere)
          case _ => Map[TicId, Set[ConditionTree]]()
        }
        
        val back = merge(merged, fromDef)
        
        d.binding match {
          case b: ReductionBinding =>
            back map { case (key, value) => key -> Set(Reduction(b, value): ConditionTree) }
          
          case _ => back
        }
      }
      
      case Where(_, left, right) => {
        val leftMap = loop(root, left, currentWhere)
        val rightMap = loop(root, right, Some(right))
        merge(leftMap, rightMap)
      }
      
      case With(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Union(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Intersect(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
            
      case Difference(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Add(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Sub(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Mul(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Div(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Mod(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Lt(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case LtEq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Gt(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case GtEq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Eq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case NotEq(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case And(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Or(_, left, right) =>
        merge(loop(root, left, currentWhere), loop(root, right, currentWhere))
      
      case Comp(_, child) => loop(root, child, currentWhere)
      
      case Neg(_, child) => loop(root, child, currentWhere)
      
      case Paren(_, child) => loop(root, child, currentWhere)
    }
    
    def loopConstraint(root: Solve)(expr: Expr): Map[TicId, Set[ConditionTree]] = {
      def listVars(expr: Expr): Set[TicId] = expr match {
        case Let(_, _, _, left, right) => listVars(left) ++ listVars(right)
        case Solve(_, _, _) => Set()
        case Import(_, _, child) => listVars(child)
        case Relate(_, from, to, in) => listVars(from) ++ listVars(to) ++ listVars(in)
        case tv @ TicVar(_, name) if tv.binding == FreeBinding(root) => Set(name)
        case TicVar(_, _) => Set()
        case StrLit(_, _) => Set()
        case NumLit(_, _) => Set()
        case BoolLit(_, _) => Set()
        case NullLit(_) => Set()
        case ObjectDef(_, props) => props map { _._2 } map listVars reduceOption { _ ++ _ } getOrElse Set()
        case ArrayDef(_, values) => values map listVars reduceOption { _ ++ _ } getOrElse Set()
        case Descent(_, child, _) => listVars(child)
        case Dispatch(_, _, actuals) => actuals map listVars reduceOption { _ ++ _ } getOrElse Set()
        case Where(_, left, right) => listVars(left) ++ listVars(right)
        case With(_, left, right) => listVars(left) ++ listVars(right)
        case Union(_, left, right) => listVars(left) ++ listVars(right)
        case Intersect(_, left, right) => listVars(left) ++ listVars(right)
        case Difference(_, left, right) => listVars(left) ++ listVars(right)
        case Add(_, left, right) => listVars(left) ++ listVars(right)
        case Sub(_, left, right) => listVars(left) ++ listVars(right)
        case Mul(_, left, right) => listVars(left) ++ listVars(right)
        case Div(_, left, right) => listVars(left) ++ listVars(right)
        case Mod(_, left, right) => listVars(left) ++ listVars(right)
        case Lt(_, left, right) => listVars(left) ++ listVars(right)
        case LtEq(_, left, right) => listVars(left) ++ listVars(right)
        case Gt(_, left, right) => listVars(left) ++ listVars(right)
        case GtEq(_, left, right) => listVars(left) ++ listVars(right)
        case Eq(_, left, right) => listVars(left) ++ listVars(right)
        case NotEq(_, left, right) => listVars(left) ++ listVars(right)
        case And(_, left, right) => listVars(left) ++ listVars(right)
        case Or(_, left, right) => listVars(left) ++ listVars(right)
        case Comp(_, child) => listVars(child)
        case Neg(_, child) => listVars(child)
        case Paren(_, child) => listVars(child)
      }
      
      expr match {
        case tv @ TicVar(_, _) if tv.binding == FreeBinding(root) =>
          Map()
        
        case _ =>
          Map(listVars(expr).toSeq map { _ -> Set[ConditionTree](Condition(expr)) }: _*)
      }
    }
    
    expr match {
      case root @ Solve(_, constraints, left) => {
        val wheres = loop(root, left, None)
        val constraintTrees = constraints map loopConstraint(root) reduceOption { merge(_, _) } getOrElse Map[TicId, Set[ConditionTree]]()
        
        merge(wheres, constraintTrees) map {
          case (key, value) => {
            val result = runAtLevels(value) { e => splitConj(e) filter referencesTicVar(root) }
            key -> result
          }
        }
      }
      case _ => Map()
    }
  }
  
  private def runAtLevels(trees: Set[ConditionTree])(f: Expr => Set[Expr]): Set[ConditionTree] = trees flatMap {
    case condition.Condition(expr) => f(expr) map { condition.Condition(_): ConditionTree }
    
    case condition.Reduction(b, trees) => {
      val rec = runAtLevels(trees)(f)
      if (rec.isEmpty)
        Set[ConditionTree]()
      else
        Set(condition.Reduction(b, rec): ConditionTree)
    }
  }
  
  private def splitConj(expr: Expr): Set[Expr] = expr match {
    case And(_, left, right) => splitConj(left) ++ splitConj(right)
    case e => Set(e)
  }
  
  private def referencesTicVar(root: Solve)(expr: Expr): Boolean = expr match {
    case Let(_, _, _, _, right) => referencesTicVar(root)(right)
    
    case New(_, child) => referencesTicVar(root)(child)
    
    case Relate(_, from, to, in) =>
      referencesTicVar(root)(from) || referencesTicVar(root)(to) || referencesTicVar(root)(in)
    
    case t @ TicVar(_, _) => t.binding match {
      case SolveBinding(`root`) => true
      case FreeBinding(`root`) => true
      case _ => false
    }
    
    case StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => false
    
    case ObjectDef(_, props) => props exists { case (_, e) => referencesTicVar(root)(e) }
    
    case ArrayDef(_, values) => values exists referencesTicVar(root)
    
    case Descent(_, child, _) => referencesTicVar(root)(child)
    
    case MetaDescent(_, child, _) => referencesTicVar(root)(child)
    
    case Deref(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case d @ Dispatch(_, _, actuals) => {
      val paramRef = actuals exists referencesTicVar(root)
      val defRef = d.binding match {
        case LetBinding(e) => referencesTicVar(root)(e.left)
        case _ => false
      }
      
      paramRef || defRef
    }
    
    case Where(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)

    case With(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)

    case Union(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)

    case Intersect(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)

    case Difference(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Add(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Sub(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Mul(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Div(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Mod(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Lt(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case LtEq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Gt(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case GtEq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Eq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case NotEq(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case And(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Or(_, left, right) => referencesTicVar(root)(left) || referencesTicVar(root)(right)
    
    case Comp(_, child) => referencesTicVar(root)(child)
    
    case Neg(_, child) => referencesTicVar(root)(child)
    
    case Paren(_, child) => referencesTicVar(root)(child)
  }
  
  
  sealed trait ConditionTree
  
  object condition {
    case class Condition(expr: Expr) extends ConditionTree
    case class Reduction(b: ReductionBinding, children: Set[ConditionTree]) extends ConditionTree
  }
}
