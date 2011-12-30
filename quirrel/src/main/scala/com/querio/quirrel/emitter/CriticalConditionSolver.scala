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
          
          val solutions = for ((name, conditions) <- work; expr <- conditions) yield {
            val result = expr match {
              case expr: Eq => solveRelation(expr) { case TicVar(_, `name`) => true }
              case _ => None
            }
            
            result map { e => Left(name -> e) } getOrElse Right(Set(Error(expr, UnableToSolveCriticalCondition(name))))
          }
          
          val errors = (solutions map { _.right getOrElse Set() }).fold(Set[Error]()) { _ ++ _ }
          val pairs = solutions flatMap { _.left.toSeq }
          
          val results = pairs.foldLeft(Map[String, Set[Expr]]()) {
            case (acc, (name, solution)) => {
              if (acc contains name)
                acc.updated(name, acc(name) + solution)
              else
                acc.updated(name, Set(solution))
            }
          }
          
          if (errors.isEmpty)
            d.criticalSolutions = results
          else
            d.criticalSolutions = Map()
          
          errors
        }
        
        case _ => {
          d.criticalSolutions = Map()
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
}
