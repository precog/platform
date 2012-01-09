package com.querio.quirrel
package emitter

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

import parser._
import typer._

trait CriticalConditionSolver extends AST with CriticalConditionFinder with Solver {
  import ast._
  
  override def solveCriticalConditions(tree: Expr): Set[Error] = tree match {
    case expr @ Let(_, _, _, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case New(_, child) => solveCriticalConditions(child)
    
    case Relate(_, from, to, in) =>
      solveCriticalConditions(from) ++ solveCriticalConditions(to) ++ solveCriticalConditions(in)
    
    case TicVar(_, _) => Set()
    case StrLit(_, _) => Set()
    case NumLit(_, _) => Set()
    case BoolLit(_, _) => Set()
    
    case ObjectDef(_, props) =>
      (props map { case (_, e) => solveCriticalConditions(e) }).fold(Set[Error]()) { _ ++ _ }
    
    case ArrayDef(_, values) =>
      (values map solveCriticalConditions).fold(Set[Error]()) { _ ++ _ }
    
    case Descent(_, child, _) => solveCriticalConditions(child)
    
    case Deref(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case d @ Dispatch(_, _, actuals) => {
      val actualErrors = (actuals map solveCriticalConditions).fold(Set[Error]()) { _ ++ _ }
      
      val ourErrors = d.binding match {
        case UserDef(let) => {
          val remaining = let.params drop actuals.length
          val work = let.criticalConditions filterKeys remaining.contains
          
          val solvedData = work map {
            case (name, forest) => name -> solveConditionForest(d, name, forest)
          }
          
          val errors = solvedData.values map { case (errorSet, _) => errorSet } flatten
          val solutions = solvedData collect {
            case (name, (_, Some(solution))) => name -> solution
          }
          
          d.equalitySolutions = solutions
          
          val finalErrors = if (remaining forall solutions.contains)
            Set()
          else
            Set(remaining filterNot solutions.contains map UnableToDetermineDefiningSet map { Error(d, _) }: _*)
          
          errors ++ finalErrors
        }
        
        case _ => {
          d.equalitySolutions = Map()
          Set()
        }
      }
      
      actualErrors ++ ourErrors
    }
    
    case Operation(_, left, _, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Add(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Sub(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Mul(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Div(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Lt(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case LtEq(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Gt(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case GtEq(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Eq(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case NotEq(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case And(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Or(_, left, right) =>
      solveCriticalConditions(left) ++ solveCriticalConditions(right)
    
    case Comp(_, child) => solveCriticalConditions(child)
    
    case Neg(_, child) => solveCriticalConditions(child)
    
    case Paren(_, child) => solveCriticalConditions(child)
  }
  
  private def solveCondition(name: String, expr: Expr): Either[Set[Error], Solution] = expr match {
    case And(_, left, right) => {
      val recLeft = solveCondition(name, left)
      val recRight = solveCondition(name, right)
      
      val sol = for {
        leftSol <- recLeft.right
        rightSol <- recRight.right
      } yield Conjunction(leftSol, rightSol)
      
      for {
        leftErr <- sol.left
        rightErr <- recRight.left
      } yield leftErr ++ rightErr
    }
    
    case Or(_, left, right) => {
      val recLeft = solveCondition(name, left)
      val recRight = solveCondition(name, right)
      
      val sol = for {
        leftSol <- recLeft.right
        rightSol <- recRight.right
      } yield Disjunction(leftSol, rightSol)
      
      for {
        leftErr <- sol.left
        rightErr <- recRight.left
      } yield leftErr ++ rightErr
    }
    
    case _ => {
      val result = expr match {
        case expr: RelationExpr => solveRelation(expr) { case TicVar(_, `name`) => true }
        case expr: Comp => solveComplement(expr) { case TicVar(_, `name`) => true }
        case _ => None
      }
      
      result map { e => Right(Definition(e)) } getOrElse Left(Set(Error(expr, UnableToSolveCriticalCondition(name))))
    }
  }
  
  private def solveConditionForest(d: Dispatch, name: String, conditions: Set[ConditionTree]): (Set[Error], Option[Solution]) = {
    if (conditions exists { case Condition(_) => true case _ => false }) {
      val solutions = conditions collect {
        case Condition(expr) => solveCondition(name, expr)
      }
      
      val successes = solutions flatMap { _.right.toSeq }
      val errors = (solutions flatMap { _.left.toSeq }).fold(Set[Error]()) { _ ++ _ }
      
      val (addend, result) = if (!successes.isEmpty)
        (None, Some(successes reduce Conjunction))
      else
        (Some(Error(d, UnableToSolveCriticalCondition(name))), None)
      
      (addend map (errors +) getOrElse errors, result)
    } else if (conditions exists { case Reduction(_, _) => true case _ => false }) {
      val (errors, successes) = conditions collect {
        case Reduction(_, children) => solveConditionForest(d, name, children)
      } unzip
      
      val result = sequence(successes) map { _ reduce Disjunction }
      
      if (result.isEmpty)
        (errors.flatten + Error(d, UnableToSolveCriticalCondition(name)), result)
      else
        (errors.flatten, result)
    } else {
      (Set(Error(d, UnableToSolveCriticalCondition(name))), None)
    }
  }
  
  private def sequence[A](set: Set[Option[A]]): Option[Set[A]] = {
    set.foldLeft(Some(Set()): Option[Set[A]]) { (acc, opt) =>
      for (s <- acc; v <- opt) yield s + v
    }
  }
  
  
  sealed trait Solution
  case class Conjunction(left: Solution, right: Solution) extends Solution
  case class Disjunction(left: Solution, right: Solution) extends Solution
  case class Definition(expr: Expr) extends Solution
}
