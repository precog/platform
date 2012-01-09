package com.querio.quirrel
package emitter

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
          
          // note: the correctness of this *depends* on pre-splitting of top-level conjunctions
          val solutions = for ((name, conditions) <- work.toSeq; Condition(expr) <- conditions)
            yield name -> solveCondition(name, expr)
          
          val (_, values) = solutions.unzip
          val errors = (values map { _.left getOrElse Set() }).fold(Set[Error]()) { _ ++ _ }
          val pairs = solutions collect { case (name, Right(sol)) => name -> sol }
          
          val results = pairs.foldLeft(Map[String, Solution]()) {
            case (acc, pair @ (name, solution)) => {
              if (acc contains name)
                acc.updated(name, Conjunction(acc(name), solution))
              else
                acc + pair
            }
          }
          
          d.equalitySolutions = results
          
          val finalErrors = if (remaining forall results.contains)
            Set()
          else
            Set(remaining filterNot results.contains map UnableToDetermineDefiningSet map { Error(d, _) }: _*)
          
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
  
  
  sealed trait Solution
  case class Conjunction(left: Solution, right: Solution) extends Solution
  case class Disjunction(left: Solution, right: Solution) extends Solution
  case class Definition(expr: Expr) extends Solution
}
