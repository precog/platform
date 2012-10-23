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
package emitter

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

import parser._
import typer._

trait GroupSolver extends AST with GroupFinder with Solver {
  import Function._
  
  import ast._
  import group._
  
  import buckets._
  
  override def inferBuckets(tree: Expr): Set[Error] = tree match {
    case Let(_, _, _, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case expr @ Solve(_, constraints, child) => {
      val constrErrors = constraints map inferBuckets reduce { _ ++ _ }
      val childErrors = inferBuckets(child)
      
      val (spec, errors) = solveForest(expr, expr.groups)(IntersectBucketSpec)
      
      val finalErrors = spec match {
        case Some(forest) => {
          val rem = expr.vars -- listSolvedVars(forest)
          rem map UnableToSolveTicVariable map { Error(expr, _) }
        }
        
        case None =>
          expr.vars map UnableToSolveTicVariable map { Error(expr, _) }
      }
      
      expr.buckets = spec
      
      constrErrors ++ childErrors ++ errors ++ finalErrors
    }
    
    case Import(_, _, child) => inferBuckets(child)
    
    case New(_, child) => inferBuckets(child)
    
    case Relate(_, from, to, in) =>
      inferBuckets(from) ++ inferBuckets(to) ++ inferBuckets(in)
    
    case TicVar(_, _) => Set()
    case StrLit(_, _) => Set()
    case NumLit(_, _) => Set()
    case BoolLit(_, _) => Set()
    case NullLit(_) => Set()
    
    case ObjectDef(_, props) =>
      (props map { case (_, e) => inferBuckets(e) }).fold(Set[Error]()) { _ ++ _ }
    
    case ArrayDef(_, values) =>
      (values map inferBuckets).fold(Set[Error]()) { _ ++ _ }
    
    case Descent(_, child, _) => inferBuckets(child)
    
    case MetaDescent(_, child, _) => inferBuckets(child)
    
    case Deref(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case d @ Dispatch(_, _, actuals) =>
      (actuals map inferBuckets).fold(Set[Error]()) { _ ++ _ }
    
    case Where(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case With(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Union(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Intersect(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
        
    case Difference(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Add(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Sub(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Mul(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Div(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Mod(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Lt(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case LtEq(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Gt(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case GtEq(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Eq(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case NotEq(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case And(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Or(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Comp(_, child) => inferBuckets(child)
    
    case Neg(_, child) => inferBuckets(child)
    
    case Paren(_, child) => inferBuckets(child)
  }
  
  private def reduceGroupConditions(conds: Set[(Option[BucketSpec], Set[Error])])(f: (BucketSpec, BucketSpec) => BucketSpec) = {
    conds.foldLeft((None: Option[BucketSpec], Set[Error]())) {
      case ((None, acc), (Some(spec), errors)) => (Some(spec), acc ++ errors)
      case ((Some(spec1), acc), (Some(spec2), errors)) => (Some(IntersectBucketSpec(spec1, spec2)), acc ++ errors)
      case ((Some(spec), acc), (None, errors)) => (Some(spec), acc ++ errors)
      case ((None, acc), (None, errors)) => (None, acc ++ errors)
    }
  }
  
  private def solveForest(b: Solve, forest: Set[GroupTree])(f: (BucketSpec, BucketSpec) => BucketSpec): (Option[BucketSpec], Set[Error]) = {
    val (spec, condErrors) = {
      val processed = forest collect {
        case GroupCondition(origin @ Where(_, target, pred)) => {
          if (listTicVars(Some(b), target).isEmpty) {
            val (result, errors) = solveGroupCondition(b, pred, false)
            (result map { Group(Some(origin), target, _) }, errors)
          } else {
            (None, Set(Error(origin, GroupTargetSetNotIndependent)))
          }
        }

        case GroupConstraint(expr) => {
          val (result, errors) = solveGroupCondition(b, expr, true)
          val commonality = result map listSolutionExprs flatMap ExprUtils.findCommonality
          
          val back = for (r <- result; c <- commonality)
            yield Group(None, c, r)

          val contribErrors = if (!back.isDefined)
            Set(Error(expr, GroupTargetSetNotIndependent))
          else
            Set()

          (back, errors ++ contribErrors)
        }
      }
      mergeSpecs(processed)(f)
    }
    
    (spec, condErrors)
  }
  
  private def solveGroupCondition(b: Solve, expr: Expr, free: Boolean): (Option[BucketSpec], Set[Error]) = expr match {
    case And(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left, free)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right, free)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield IntersectBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case Or(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left, free)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right, free)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield UnionBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case expr: RelationExpr if !listTicVars(Some(b), expr).isEmpty => {
      val vars = listTicVars(Some(b), expr)
      
      if (vars.size > 1) {
        val ticVars = vars map { case (_, id) => id }
        (None, Set(Error(expr, InseparablePairedTicVariables(ticVars))))
      } else {
        val tv = vars.head._2
        val result = solveRelation(expr) { case t @ TicVar(_, `tv`) => !free && t.binding == SolveBinding(b) || free && t.binding == FreeBinding(b) }
        
        if (result.isDefined)
          (result map { UnfixedSolution(tv, _) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }
    
    case expr: Comp if !listTicVars(Some(b), expr).isEmpty => {
      val vars = listTicVars(Some(b), expr)
      
      if (vars.size > 1) {
        val ticVars = vars map { case (_, id) => id }
        (None, Set(Error(expr, InseparablePairedTicVariables(ticVars))))
      } else {
        val tv = vars.head._2
        val result = solveComplement(expr) { case t @ TicVar(_, `tv`) => !free && t.binding == SolveBinding(b) || free && t.binding == FreeBinding(b) }
        
        if (result.isDefined)
          (result map { UnfixedSolution(tv, _) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }

    case expr if listTicVars(Some(b), expr).isEmpty && !listTicVars(None, expr).isEmpty =>
      (None, Set(Error(expr, ConstraintsWithinInnerScope)))
    
    case _ if listTicVars(Some(b), expr).isEmpty => 
      (Some(Extra(expr)), Set())
    
    case _ => (None, listTicVars(Some(b), expr) map { case (_, id) => id } map UnableToSolveTicVariable map { Error(expr, _) })
  }
  
  private def mergeSpecs(specs: TraversableOnce[(Option[BucketSpec], Set[Error])])(f: (BucketSpec, BucketSpec) => BucketSpec): (Option[BucketSpec], Set[Error]) = {
    val (back, errors) = specs.fold((None: Option[BucketSpec], Set[Error]())) {
      case ((leftAcc, leftErrors), (rightAcc, rightErrors)) => {
        val merged = for (left <- leftAcc; right <- rightAcc)
          yield f(left, right)
        
        (merged orElse leftAcc orElse rightAcc, leftErrors ++ rightErrors)
      }
    }

    if (f == IntersectBucketSpec && back.isDefined && !(errors collect { case Error(tpe) => tpe } contains ConstraintsWithinInnerScope))
      (back, Set())
    else
      (back, errors)
  }
  
  //if b is Some: finds all tic vars in the Expr that have the given Solve as their binding
  //if b is None: finds all tic vars in the Expr
  private def listTicVars(b: Option[Solve], expr: Expr): Set[(Option[Solve], TicId)] = expr match {
    case Let(_, _, _, left, right) => listTicVars(b, right)
    
    case b2 @ Solve(_, constraints, child) => {
      val allVars = (constraints map { listTicVars(b, _) } reduce { _ ++ _ })
      allVars -- listTicVars(Some(b2), child)
    }
    
    case New(_, child) => listTicVars(b, child)
    case Relate(_, from, to, in) => listTicVars(b, from) ++ listTicVars(b, to) ++ listTicVars(b, in)
    
    case t @ TicVar(_, name) if b.isDefined && (t.binding == SolveBinding(b.get) || t.binding == FreeBinding(b.get)) => {
      t.binding match {
        case SolveBinding(b2) => Set((Some(b2), name)) 
        case FreeBinding(b2) => Set((Some(b2), name)) 
        case NullBinding => Set()
      }
    }
    
    case t @ TicVar(_, name) if !b.isDefined => {
      t.binding match {
        case SolveBinding(b2) => Set((Some(b2), name)) 
        case FreeBinding(b2) => Set((Some(b2), name)) 
        case NullBinding => Set()
      }
    }
    
    case TicVar(_, _) => Set()
    case StrLit(_, _) => Set()
    case NumLit(_, _) => Set()
    case BoolLit(_, _) => Set()
    case NullLit(_) => Set()
    case ObjectDef(_, props) => (props.unzip._2 map { listTicVars(b, _) }).fold(Set()) { _ ++ _ }
    case ArrayDef(_, values) => (values map { listTicVars(b, _) }).fold(Set()) { _ ++ _ }
    case Descent(_, child, _) => listTicVars(b, child)
    case MetaDescent(_, child, _) => listTicVars(b, child)
    case Deref(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    
    case d @ Dispatch(_, _, actuals) => {
      val leftSet = d.binding match {
        case LetBinding(b2) => listTicVars(b, b2.left)
        case _ => Set[(Option[Solve], TicId)]()
      }
      (actuals map { listTicVars(b, _) }).fold(leftSet) { _ ++ _ }
    }
    
    case Where(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case With(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Union(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Intersect(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Add(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Sub(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Mul(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Div(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Mod(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Lt(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case LtEq(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Gt(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case GtEq(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Eq(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case NotEq(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case And(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Or(_, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Comp(_, child) => listTicVars(b, child)
    case Neg(_, child) => listTicVars(b, child)
    case Paren(_, child) => listTicVars(b, child)
  }
  
  private def listSolvedVars(spec: BucketSpec): Set[TicId] = spec match {
    case UnionBucketSpec(left, right) => listSolvedVars(left) ++ listSolvedVars(right)
    case IntersectBucketSpec(left, right) => listSolvedVars(left) ++ listSolvedVars(right)
    case Group(_, _, forest) => listSolvedVars(forest)
    case UnfixedSolution(id, _) => Set(id)
    case FixedSolution(id, _, _) => Set(id)
    case Extra(_) => Set()
  }

  private def listSolutionExprs(spec: BucketSpec): Set[Expr] = spec match {
    case UnionBucketSpec(left, right) => listSolutionExprs(left) ++ listSolutionExprs(right)
    case IntersectBucketSpec(left, right) => listSolutionExprs(left) ++ listSolutionExprs(right)
    case Group(_, _, forest) => listSolutionExprs(forest)
    case UnfixedSolution(_, expr) => Set(expr)
    case FixedSolution(_, solution, _) => Set(solution)
    case Extra(expr) => Set(expr)
  }
  

  sealed trait BucketSpec {
    import buckets._
    
    final def derive(id: TicId, expr: Expr): BucketSpec = this match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(left.derive(id, expr), right.derive(id, expr))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(left.derive(id, expr), right.derive(id, expr))
      
      case Group(origin, target, forest) =>
        Group(origin, target, forest.derive(id, expr))
      
      case UnfixedSolution(`id`, solution) =>
        FixedSolution(id, solution, expr)
      
      case s @ UnfixedSolution(_, _) => s
      
      case f @ FixedSolution(id2, _, _) => {
        assert(id != id2)
        f
      }
      
      case e @ Extra(_) => e
    }
  }
  
  object buckets {
    case class UnionBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    case class IntersectBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    
    case class Group(origin: Option[Where], target: Expr, forest: BucketSpec) extends BucketSpec
    
    case class UnfixedSolution(id: TicId, solution: Expr) extends BucketSpec
    case class FixedSolution(id: TicId, solution: Expr, expr: Expr) extends BucketSpec
    
    case class Extra(expr: Expr) extends BucketSpec
  }
}
