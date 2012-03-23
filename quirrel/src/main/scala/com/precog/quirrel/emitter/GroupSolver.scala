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

trait GroupSolver extends AST with GroupFinder with Solver with Solutions {
  import Function._
  
  import ast._
  import group._
  
  override def inferBuckets(tree: Expr): Set[Error] = tree match {
    case expr @ Let(_, _, _, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case New(_, child) => inferBuckets(child)
    
    case Relate(_, from, to, in) =>
      inferBuckets(from) ++ inferBuckets(to) ++ inferBuckets(in)
    
    case TicVar(_, _) => Set()
    case StrLit(_, _) => Set()
    case NumLit(_, _) => Set()
    case BoolLit(_, _) => Set()
    
    case ObjectDef(_, props) =>
      (props map { case (_, e) => inferBuckets(e) }).fold(Set[Error]()) { _ ++ _ }
    
    case ArrayDef(_, values) =>
      (values map inferBuckets).fold(Set[Error]()) { _ ++ _ }
    
    case Descent(_, child, _) => inferBuckets(child)
    
    case Deref(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case d @ Dispatch(_, _, actuals) => {
      val actualErrors = (actuals map inferBuckets).fold(Set[Error]()) { _ ++ _ }
      
      val ourErrors = d.binding match {
        case UserDef(let) => {
          val remaining = let.params drop actuals.length
          val work = let.groups filterKeys remaining.contains
          
          val solvedData = work map {
            case (name, forest) => name -> solveGroupForest(d, name, forest)
          }
          
          val errors = solvedData.values map { case (errorSet, _) => errorSet } flatten
          val solutions = solvedData collect {
            case (name, (_, Some(solution))) => name -> solution
          }
          
          d.buckets = solutions
          
          val finalErrors = if (remaining forall solutions.contains)
            Set()
          else
            Set(remaining filterNot solutions.contains map UnableToDetermineDefiningSet map { Error(d, _) }: _*)
          
          errors ++ finalErrors
        }
        
        case _ => {
          d.buckets = Map()
          Set()
        }
      }
      
      actualErrors ++ ourErrors
    }
    
    case Where(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case With(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Union(_, left, right) =>
      inferBuckets(left) ++ inferBuckets(right)
    
    case Intersect(_, left, right) =>
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
  
  private def solveCondition(name: String, expr: Expr): Either[Set[Error], (Option[Solution], Set[Expr])] = expr match {
    case And(_, left, right) => {
      val recLeft = solveCondition(name, left)
      val recRight = solveCondition(name, right)
      
      // desugared due to SI-5589
      val sol = recLeft.right flatMap {
        case (leftSol, leftExtras) => {
          recRight.right map {
            case (rightSol, rightExtras) => {
              val merged = for (a <- leftSol; b <- rightSol) yield Conjunction(a, b)
              val completeSol = merged orElse leftSol orElse rightSol
              
              val leftExtras2 = leftSol map const(leftExtras) getOrElse Set(left)
              val rightExtras2 = rightSol map const(rightExtras) getOrElse Set(right)
              
              (completeSol, leftExtras2 ++ rightExtras2)
            }
          }
        }
      }
      
      // TODO this misses errors sometimes
      for {
        leftErr <- sol.left
        rightErr <- recRight.left
      } yield leftErr ++ rightErr
    }
    
    case Or(_, left, right) => {
      val recLeft = solveCondition(name, left)
      val recRight = solveCondition(name, right)
      
      // desugared due to SI-5589
      val maybeSol = recLeft.right flatMap {
        case (leftSol, leftExtras) => {
          recRight.right map {
            case (rightSol, rightExtras) => {
              for {
                a <- leftSol
                b <- rightSol
                
                if leftExtras.isEmpty
                if rightExtras.isEmpty
              } yield (Disjunction(a, b), Set[Expr]())
            }
          }
        }
      }
      
      val sol = maybeSol.right flatMap {
        case Some((solution, errors)) => Right((Some(solution), errors))
        case None => Left(Set(Error(expr, UnableToSolveCriticalCondition(name))))
      }
      
      // TODO this misses errors sometimes
      for {
        leftErr <- sol.left
        rightErr <- recRight.left
      } yield leftErr ++ rightErr
    }
    
    case _ if containsTicVar(name)(expr) => {
      val result = expr match {
        case expr: RelationExpr => solveRelation(expr) { case TicVar(_, `name`) => true }
        case expr: Comp => solveComplement(expr) { case TicVar(_, `name`) => true }
        case _ => None
      }
      
      result map { e => Right((Some(Definition(e)), Set[Expr]())) } getOrElse Left(Set(Error(expr, UnableToSolveCriticalCondition(name))))
    }
    
    case _ if containsAnotherTicVar(name)(expr) =>
      Left(Set(Error(expr, GroupSetInvolvingMultipleParameters)))
    
    case _ => Right((None, Set(expr)))
  }
  
  private def solveGroupForest(d: Dispatch, name: String, groups: Set[GroupTree]): (Set[Error], Option[Bucket]) = {
    if (groups exists { case Condition(_) => true case _ => false }) {
      val solutions = groups collect {
        case Condition(where) => (where, solveCondition(name, where.right))
      }
      
      // if it's None all the way down, then we already have the error
      val successes = for ((expr, result) <- solutions; (Some(solution), extras) <- result.right.toSeq)
        yield Group(expr, expr.left, solution, extras)
      
      val errors = (solutions flatMap { _._2.left.toSeq }).fold(Set[Error]()) { _ ++ _ }
      
      val (addend, result) = if (!successes.isEmpty)
        (None, Some(successes reduce IntersectBucket))
      else
        (Some(Error(d, UnableToDetermineDefiningSet(name))), None)
      
      (addend map (errors +) getOrElse errors, result)
    } else if (groups exists { case Reduction(_, _) => true case _ => false }) {
      val (errors, successes) = groups collect {
        case Reduction(_, children) => solveGroupForest(d, name, children)
      } unzip
      
      val result = sequence(successes) map { _ reduce UnionBucket }
      
      if (result.isEmpty)
        (errors.flatten + Error(d, UnableToDetermineDefiningSet(name)), result)
      else
        (errors.flatten, result)
    } else {
      (Set(Error(d, UnableToDetermineDefiningSet(name))), None)
    }
  }
  
  private def containsTicVar(name: String): Expr => Boolean = isSubtree {
    case TicVar(_, `name`) => true
    case _ => false
  }
  
  private def containsAnotherTicVar(name: String): Expr => Boolean = isSubtree {
    case TicVar(_, name2) => name != name2
    case _ => false
  }
  
  private def sequence[A](set: Set[Option[A]]): Option[Set[A]] = {
    set.foldLeft(Some(Set()): Option[Set[A]]) { (acc, opt) =>
      for (s <- acc; v <- opt) yield s + v
    }
  }
  
  sealed trait Bucket
  
  case class UnionBucket(left: Bucket, right: Bucket) extends Bucket
  case class IntersectBucket(left: Bucket, right: Bucket) extends Bucket
  case class Group(origin: Where, target: Expr, forest: Solution, extras: Set[Expr]) extends Bucket
}
