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
    case expr @ Let(_, _, params, left, right) => {
      val leftErrors = inferBuckets(left)
      
      val (spec, errors) = solveForest(expr, expr.groups)(IntersectBucketSpec)
      
      val finalErrors = spec match {
        case Some(forest) => {
          val rem = Set(params: _*) -- listSolvedVars(forest)
          rem map UnableToSolveTicVariable map { Error(expr, _) }
        }
        
        case None => {
          Set(params map UnableToSolveTicVariable map { Error(expr, _) }: _*)
        }
      }
      
      expr.buckets = spec
      
      leftErrors ++ errors ++ finalErrors ++ inferBuckets(right)
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
  
  private def solveForest(b: Let, forest: Set[GroupTree])(f: (BucketSpec, BucketSpec) => BucketSpec): (Option[BucketSpec], Set[Error]) = {
    val (conditions, reductions) = forest partition {
      case c: GroupCondition => true
      case r: GroupReduction => false
    }
    
    val (optCondSpec, condErrors) = {
      val processed = conditions collect {
        case GroupCondition(origin @ Where(_, target, pred)) => {
          if (listTicVars(b, target).isEmpty) {
            val (result, errors) = solveGroupCondition(b, pred)
            (result map { Group(origin, target, _) }, errors)
          } else {
            (None, Set(Error(origin, GroupTargetSetNotIndependent)))
          }
        }
      }
      mergeSpecs(processed)(f)
    }
    
    val typedReductions = reductions collect { case r: GroupReduction => r }
    
    val (optRedSpec, redErrors) = mergeSpecs(typedReductions map solveReduction(b))(UnionBucketSpec)
    
    val andSpec = for (cond <- optCondSpec; red <- optRedSpec)
      yield UnionBucketSpec(cond, red)
    
    val spec = andSpec orElse optCondSpec orElse optRedSpec
    
    (spec, if (optCondSpec.isDefined) condErrors else condErrors ++ redErrors)
  }
  
  private def solveGroupCondition(b: Let, expr: Expr): (Option[BucketSpec], Set[Error]) = expr match {
    case And(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield IntersectBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case Or(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield UnionBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case expr: RelationExpr if !listTicVars(b, expr).isEmpty => {
      val vars = listTicVars(b, expr)
      
      if (vars.size > 1) {
        (None, Set(Error(expr, InseparablePairedTicVariables(vars))))
      } else {
        val tv = vars.head
        val result = solveRelation(expr) { case t @ TicVar(_, `tv`) => t.binding == LetBinding(b) }
        
        if (result.isDefined)
          (result map { UnfixedSolution(tv, _) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }
    
    case expr: Comp if !listTicVars(b, expr).isEmpty => {
      val vars = listTicVars(b, expr)
      
      if (vars.size > 1) {
        (None, Set(Error(expr, InseparablePairedTicVariables(vars))))
      } else {
        val tv = vars.head
        val result = solveComplement(expr) { case t @ TicVar(_, `tv`) => t.binding == LetBinding(b) }
        
        if (result.isDefined)
          (result map { UnfixedSolution(tv, _) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }
    
    case _ if listTicVars(b, expr).isEmpty => (Some(Extra(expr)), Set())
    
    case _ => (None, listTicVars(b, expr) map UnableToSolveTicVariable map { Error(expr, _) })
  }
  
  private def solveReduction(b: Let)(red: GroupReduction): (Option[BucketSpec], Set[Error]) = 
    solveForest(b, red.children)(IntersectBucketSpec)
  
  private def mergeSpecs(specs: TraversableOnce[(Option[BucketSpec], Set[Error])])(f: (BucketSpec, BucketSpec) => BucketSpec): (Option[BucketSpec], Set[Error]) = {
    val optionalErrors = f == IntersectBucketSpec
    
    val (back, errors) = specs.fold((None: Option[BucketSpec], Set[Error]())) {
      case ((leftAcc, leftErrors), (rightAcc, rightErrors)) => {
        val merged = for (left <- leftAcc; right <- rightAcc)
          yield f(left, right)
        
        (merged orElse leftAcc orElse rightAcc, leftErrors ++ rightErrors)
      }
    }
    
    if (f == IntersectBucketSpec && back.isDefined)
      (back, Set())
    else
      (back, errors)
  }
  
  private def listTicVars(b: Let, expr: Expr): Set[TicId] = expr match {
    case Let(_, _, _, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case New(_, child) => listTicVars(b, child)
    case Relate(_, from, to, in) => listTicVars(b, from) ++ listTicVars(b, to) ++ listTicVars(b, in)
    case t @ TicVar(_, name) if t.binding == LetBinding(b) => Set(name)
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
        case _ => Set[TicId]()
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
    
    case class Group(origin: Where, target: Expr, forest: BucketSpec) extends BucketSpec
    
    case class UnfixedSolution(id: TicId, solution: Expr) extends BucketSpec
    case class FixedSolution(id: TicId, solution: Expr, expr: Expr) extends BucketSpec
    
    case class Extra(expr: Expr) extends BucketSpec
  }
}
