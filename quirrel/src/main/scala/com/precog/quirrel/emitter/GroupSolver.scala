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

import scala.annotation.tailrec

import scala.collection.GenTraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

import com.codecommit.gll.ast.Node

import parser._
import typer._

trait GroupSolver extends AST with GroupFinder with Solver with ProvenanceChecker {
  import Function._
  
  import ast._
  import buckets._
  
  override def inferBuckets(tree: Expr): Set[Error] = {
    def loopSpec(dispatches: Set[Dispatch])(spec: BucketSpec): Set[Error] = spec match {
      case UnionBucketSpec(left, right) =>
        loopSpec(dispatches)(left) ++ loopSpec(dispatches)(right)
      
      case IntersectBucketSpec(left, right) =>
        loopSpec(dispatches)(left) ++ loopSpec(dispatches)(right)
      
      case Group(origin, target, forest, _) => {
        val originErrors = origin map loop(dispatches) getOrElse Set()
        val targetErrors = loop(dispatches)(target)
        val forestErrors = loopSpec(dispatches)(forest)
        
        originErrors ++ targetErrors ++ forestErrors
      }
      
      case UnfixedSolution(_, solution, _) => loop(dispatches)(solution)
      
      case FixedSolution(_, solution, expr, _) =>
        loop(dispatches)(solution) ++ loop(dispatches)(expr)
      
      case Extra(expr, _) => loop(dispatches)(expr)
    }
    
    def loop(dispatches: Set[Dispatch])(tree: Expr): Set[Error] = tree match {
      case Let(_, _, _, _, right) => loop(dispatches)(right)
      
      case expr @ Solve(_, constraints, child) => {
        // this can populate buckets with more dispatches than emitter will
        // have.  that's ok though, because we *add* the correct dispatch set
        // below when we use loopSpec
        val childErrors = loop(dispatches)(child)
        
        val sigma: Sigma = dispatches flatMap { dispatch =>
          val (subs, letM) = dispatch.binding match {
            case LetBinding(let) => (let.substitutions, Some(let))
            case _ => (Map[Dispatch, Map[String, Expr]](), None)
          }
          
          letM map { let =>
            subs(dispatch) map {
              case (key, value) => (Identifier(Vector(), key) -> let) -> value
            }
          }
        } reduceOption { _ ++ _ } getOrElse Map()
        
        val (forestSpecM, forestErrors) = solveForest(expr, findGroups(expr, dispatches))
        
        val filtered = constraints filter {
          case TicVar(_, _) => false
          case _ => true
        }
        
        val (constrSpecM, constrErrors) = mergeSpecs(filtered map { solveConstraint(expr, _, sigma, Nil) })
        
        val mergedM = for (forestSpec <- forestSpecM; constrSpec <- constrSpecM)
          yield IntersectBucketSpec(forestSpec, constrSpec)
        
        val specM = mergedM orElse forestSpecM orElse constrSpecM
        
        val finalErrors = specM match {
          case Some(forest) => {
            val rem = expr.vars -- listSolvedVars(forest)
            rem map UnableToSolveTicVariable map { Error(expr, _) }
          }
          
          case None =>
            expr.vars map UnableToSolveTicVariable map { Error(expr, _) }
        }
        
        val specErrors = specM map { spec =>
          expr buckets_+= (dispatches -> spec)
          
          loopSpec(dispatches)(spec)
        } getOrElse Set()
        
        val forestErrors2 = if (finalErrors.isEmpty) {
          forestErrors filter {
            case Error(tpe) => tpe == ConstraintsWithinInnerSolve
          }
        } else {
          forestErrors
        }
        
        childErrors ++ specErrors ++ forestErrors2 ++ constrErrors ++ finalErrors
      }
      
      case Assert(_, pred, child) =>
        loop(dispatches)(pred) ++ loop(dispatches)(child)
      
      case Observe(_, data, samples) =>
        loop(dispatches)(data) ++ loop(dispatches)(samples)
      
      case New(_, child) => loop(dispatches)(child)
      
      case Relate(_, from, to, in) =>
        loop(dispatches)(from) ++ loop(dispatches)(to) ++ loop(dispatches)(in)
      
      case TicVar(_, _) => Set()
      
      case d @ Dispatch(_, _, actuals) => {
        val actualErrors = (actuals map loop(dispatches)).fold(Set[Error]()) { _ ++ _ }
        
        val originErrors = d.binding match {
          case LetBinding(let) => loop(dispatches + d)(let.left)
          case _ => Set()
        }
        
        actualErrors ++ originErrors
      }
      
      case NaryOp(_, values) =>
        (values map loop(dispatches)).fold(Set[Error]()) { _ ++ _ }
    }
    
    loop(Set())(tree)
  }
  
  private def solveForest(solve: Solve, forest: Set[(Sigma, Where, List[Dispatch])]): (Option[BucketSpec], Set[Error]) = {
    val results = forest map {
      case (sigma, where, dtrace) => {
        val leftProv = where.left.provenance
        val rightProv = where.right.provenance
        
        val orderedSigma = orderTopologically(sigma)
        
        val resolvedLeftProv = orderedSigma.foldLeft(leftProv) {
          case (prov, formal @ (id, let)) => substituteParam(id, let, prov, sigma(formal).provenance)
        }
        
        val resolvedRightProv = orderedSigma.foldLeft(rightProv) {
          case (prov, formal @ (id, let)) => substituteParam(id, let, prov, sigma(formal).provenance)
        }
        
        val fullyResolvedLeftProv = resolveUnifications(where.left.relations)(resolvedLeftProv.makeCanonical)
        val fullyResolvedRightProv = resolveUnifications(where.right.relations)(resolvedRightProv.makeCanonical)
        
        if (fullyResolvedLeftProv == fullyResolvedRightProv) {
          // 1. make coffee
          // 2. attempt to solve where.right
          
          val (groupM, errors) = solveGroupCondition(solve, where.right, false, sigma, dtrace)
          
          groupM map { group =>
            val commonalityM = findCommonality(solve, group.exprs + where.left, sigma)
            
            if (commonalityM.isDefined)
              (Some(Group(Some(where), resolveExpr(sigma, where.left), group, dtrace)), errors)
            else
              (None, errors)      // TODO emit a new error
          } getOrElse (None, errors)
        } else {
          (None, Set[Error]())
        }
      }
    }
    
    mergeSpecs(results)
  }
  
  private def resolveExpr(sigma: Sigma, expr: Expr): Expr = expr match {
    case expr @ Dispatch(_, id, _) => {
      expr.binding match {
        case FormalBinding(let) => resolveExpr(sigma, sigma((id, let)))
        case _ => expr
      }
    }
    
    case _ => expr
  }
  
  private def solveConstraint(b: Solve, constraint: Expr, sigma: Sigma, dtrace: List[Dispatch]): (Option[BucketSpec], Set[Error]) = {
    val (result, errors) = solveGroupCondition(b, constraint, true, sigma, dtrace)
    
    val orderedSigma = orderTopologically(sigma)
    val commonality = result map listSolutionExprs flatMap { findCommonality(b, _, sigma) }
    
    val back = for (r <- result; c <- commonality)
      yield Group(None, c, r, List())
    
    val contribErrors = if (!back.isDefined) {
      val vars = listTicVars(Some(b), constraint, sigma) map { _._2 }
      
      if (vars.isEmpty)
        Set(Error(constraint, SolveLackingFreeVariables))
      else if (vars.size == 1)
        Set(Error(constraint, UnableToSolveCriticalCondition(vars.head)))
      else
        Set(Error(constraint, InseparablePairedTicVariables(vars)))
    } else {
      Set()
    }
    
    (back, errors ++ contribErrors)
  }
  
  private def orderTopologically(sigma: Sigma): List[Formal] = {
    val edges: Map[Formal, Set[Formal]] = sigma mapValues { expr =>
      expr.provenance.possibilities collect {
        case ParamProvenance(id, let) => (id, let)
      }
    }
    
    def bfs(vertices: Set[Formal]): List[Formal] = {
      val vertices2 = edges filter {
        case (_, targets) => !(vertices & targets).isEmpty
      } keySet
      
      if (vertices2.isEmpty)
        vertices.toList
      else
        vertices.toList ::: bfs(vertices2)
    }
    
    val leaves = edges filter {
      case (_, targets) => targets.isEmpty
    } keySet
    
    bfs(leaves).reverse
  }
  
  private def solveGroupCondition(b: Solve, expr: Expr, free: Boolean, sigma: Sigma, dtrace: List[Dispatch]): (Option[BucketSpec], Set[Error]) = expr match {
    case And(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left, free, sigma, dtrace)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right, free, sigma, dtrace)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield IntersectBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case Or(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left, free, sigma, dtrace)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right, free, sigma, dtrace)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield UnionBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case expr: ComparisonOp if !listTicVars(Some(b), expr, sigma).isEmpty => {
      val vars = listTicVars(Some(b), expr, sigma)
      
      if (vars.size > 1) {
        val ticVars = vars map { case (_, id) => id }
        (None, Set(Error(expr, InseparablePairedTicVariables(ticVars))))
      } else {
        val tv = vars.head._2
        val resultM = solveRelation(expr, sigma)(pred(b, tv, free, sigma))
        
        resultM map { result =>
          val innerVars = listTicVars(Some(b), result, sigma)
          
          if (innerVars.isEmpty) {
            (Some(UnfixedSolution(tv, result, dtrace)), Set[Error]())
          } else {
            val errors = innerVars map {
              case (_, id) => Error(expr, ExtraVarsInGroupConstraint(id))
            }
            
            (None, errors)
          }
        } getOrElse (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }
    
    case expr: Comp if !listTicVars(Some(b), expr, sigma).isEmpty => {
      val vars = listTicVars(Some(b), expr, sigma)
      
      if (vars.size > 1) {
        val ticVars = vars map { case (_, id) => id }
        (None, Set(Error(expr, InseparablePairedTicVariables(ticVars))))
      } else {
        val tv = vars.head._2
        val result = solveComplement(expr, sigma)(pred(b, tv, free, sigma))
        
        if (result.isDefined)
          (result map { UnfixedSolution(tv, _, dtrace) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }
    
    case expr @ Dispatch(_, id, actuals) => {
      expr.binding match {
        case LetBinding(let) =>
          solveGroupCondition(b, let.left, free, enterLet(sigma, let, actuals), expr :: dtrace)
        
        case FormalBinding(let) => {
          val actualM = sigma get ((id, let))
          val resultM = actualM map { solveGroupCondition(b, _, free, sigma, dtrace) }
          resultM getOrElse sys.error("uh...?")
        }
        
        // TODO solve through functions that we deeply understand
        case _ => {
          val vars = listTicVars(Some(b), expr, sigma)
          
          if (vars.isEmpty)     // it's an independent extra; we're saved!
            (Some(Extra(expr, dtrace)), Set())
          else
            (None, Set(Error(expr, UnableToSolveCriticalConditionAnon)))
        }
      }
    }

    case expr if listTicVars(Some(b), expr, sigma).isEmpty && !listTicVars(None, expr, sigma).isEmpty =>
      (None, Set(Error(expr, ConstraintsWithinInnerSolve)))
    
    case _ if listTicVars(Some(b), expr, sigma).isEmpty => 
      (Some(Extra(expr, dtrace)), Set())
    
    case _ => (None, listTicVars(Some(b), expr, sigma) map { case (_, id) => id } map UnableToSolveTicVariable map { Error(expr, _) })
  }
  
  // my appologies to humanity...
  private def pred(b: Solve, tv: TicId, free: Boolean, sigma: Sigma): PartialFunction[Node, Boolean] = {
    case d @ Dispatch(_, id, actuals) => {
      d.binding match {
        case FormalBinding(let) => {
          val actualM = sigma get ((id, let))
          actualM flatMap pred(b, tv, free, sigma).lift getOrElse false
        }
        
        case LetBinding(let) =>
          pred(b, tv, free, enterLet(sigma, let, actuals)).lift(let.left) getOrElse false
        
        case _ => false
      }
    }
    
    case t @ TicVar(_, `tv`) => !free && t.binding == SolveBinding(b) || free && t.binding == FreeBinding(b)
  }
  
  private def enterLet(sigma: Sigma, let: Let, actuals: Vector[Expr]): Sigma = {
    val ids = let.params map { Identifier(Vector(), _) }
    sigma ++ (ids zip Stream.continually(let) zip actuals)
  }
  
  private def mergeSpecs(specs: TraversableOnce[(Option[BucketSpec], Set[Error])]): (Option[BucketSpec], Set[Error]) = {
    val (back, errors) = specs.fold((None: Option[BucketSpec], Set[Error]())) {
      case ((leftAcc, leftErrors), (rightAcc, rightErrors)) => {
        val merged = for (left <- leftAcc; right <- rightAcc)
          yield IntersectBucketSpec(left, right)
        
        (merged orElse leftAcc orElse rightAcc, leftErrors ++ rightErrors)
      }
    }

    if (back.isDefined && !(errors collect { case Error(tpe) => tpe } contains ConstraintsWithinInnerSolve))
      (back, Set())
    else
      (back, errors)
  }
  
  private def isTranspecable(to: Expr, from: Expr, sigma: Sigma): Boolean = {
    to match {
      case _ if to equalsIgnoreLoc from => true
      
      case Let(_, _, _, _, right) => isTranspecable(right, from, sigma)
      
      case Relate(_, _, _, in) => isTranspecable(in, from, sigma)
      
      case Cond(_, pred, left, right) =>
        isTranspecable(pred, from, sigma) && isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      
      case to @ Dispatch(_, id, actuals) => {
        to.binding match {
          case FormalBinding(let) => {
            val exactResult = sigma get ((id, let)) map { isTranspecable(_, from, sigma) }
            
            exactResult getOrElse {
              // if we can't get the exact actual from our sigma, we have to over-
              // approximate by taking the full set of all possible dispatches and
              // ensuring that they *all* satisfy the requisite property
              let.dispatches forall { d =>
                val subSigma = Map(let.params zip d.actuals: _*)
                isTranspecable(subSigma(id.id), from, sigma)
              }
            }
          }
          
          case LetBinding(let) =>
            isTranspecable(let.left, from, enterLet(sigma, let, actuals))
          
          case Op1Binding(_) | Op2Binding(_) => true
          
          case _ => false
        }
      }
      
      case Eq(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Eq(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case NotEq(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case NotEq(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case ObjectDef(_, props) =>
        props map { _._2 } forall { isTranspecable(_, from, sigma) }
      
      case ArrayDef(_, values) =>
        values forall { isTranspecable(_, from, sigma) }
      
      case Descent(_, child, _) => isTranspecable(child, from, sigma)
      case MetaDescent(_, child, _) => isTranspecable(child, from, sigma)
      
      case Deref(_, left, right) if isPrimitive(right, sigma) =>
        isTranspecable(left, from, sigma)
      
      case Where(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Where(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case With(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case With(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Add(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Add(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Sub(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Sub(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Mul(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Mul(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Div(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Div(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Mod(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Mod(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Pow(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Pow(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)

      case Lt(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Lt(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case LtEq(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case LtEq(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Gt(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Gt(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case GtEq(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case GtEq(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case And(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case And(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      case Or(_, left, right) if isPrimitive(left, sigma) => isTranspecable(right, from, sigma)
      case Or(_, left, right) if isPrimitive(right, sigma) => isTranspecable(left, from, sigma)
      
      // non-primitive cases
      
      case Deref(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      
      case Where(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case With(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      
      case Add(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case Sub(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case Mul(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case Div(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case Mod(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case Pow(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      
      case Lt(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case LtEq(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case Gt(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case GtEq(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      
      case Eq(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case NotEq(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      
      case And(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      case Or(_, left, right) => isTranspecable(left, from, sigma) && isTranspecable(right, from, sigma)
      
      case UnaryOp(_, child) => isTranspecable(child, from, sigma)
      
      case _ => false
    }
  }
  
  private def isPrimitive(expr: Expr, sigma: Sigma): Boolean = expr match {
    case Literal(_) => true
    
    case expr @ Dispatch(_, id, actuals) => {
      expr.binding match {
        case FormalBinding(let) => {
          val exactResult = sigma get ((id, let)) map { isPrimitive(_, sigma) }
          
          exactResult getOrElse {
            // if we can't get the exact actual from our sigma, we have to over-
            // approximate by taking the full set of all possible dispatches and
            // ensuring that they *all* satisfy the requisite property
            let.dispatches forall { d =>
              val subSigma = Map(let.params zip d.actuals: _*)
              isPrimitive(subSigma(id.id), sigma)
            }
          }
        }
        
        case LetBinding(let) =>
          isPrimitive(let.left, enterLet(sigma, let, actuals))
        
        case _ => false
      }
    }
    
    case Let(_, _, _, _, right) => isPrimitive(right, sigma)
    
    case Relate(_, _, _, in) => isPrimitive(in, sigma)
    
    case _: Union | _: Intersect | _: Difference | _: Cond => false
    
    // TODO replace with NaryOp(_, values) once scalac is actually fixed
    case expr: NaryOp =>
      expr.values forall { isPrimitive(_, sigma) }
    
    case _ => false
  }
  
  //if b is Some: finds all tic vars in the Expr that have the given Solve as their binding
  //if b is None: finds all tic vars in the Expr
  private def listTicVars(b: Option[Solve], expr: Expr, sigma: Sigma): Set[(Option[Solve], TicId)] = expr match {
    case Let(_, _, _, left, right) => listTicVars(b, right, sigma)
    
    case b2 @ Solve(_, constraints, child) => {
      val allVars = (constraints map { listTicVars(b, _, sigma) } reduce { _ ++ _ })
      allVars -- listTicVars(Some(b2), child, sigma)
    }
    
    case Assert(_, pred, child) => listTicVars(b, pred, sigma) ++ listTicVars(b, child, sigma)
    
    case Observe(_, data, samples) => listTicVars(b, data, sigma) ++ listTicVars(b, samples, sigma)
    
    case New(_, child) => listTicVars(b, child, sigma)
    
    case Relate(_, from, to, in) => listTicVars(b, from, sigma) ++ listTicVars(b, to, sigma) ++ listTicVars(b, in, sigma)
    
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
    
    case d @ Dispatch(_, id, actuals) => {
      val leftSet = d.binding match {
        case LetBinding(b2) =>
          listTicVars(b, b2.left, enterLet(sigma, b2, actuals))
        
        case FormalBinding(let) => listTicVars(b, sigma((id, let)), sigma)
        case _ => Set[(Option[Solve], TicId)]()
      }
      (actuals map { listTicVars(b, _, sigma) }).fold(leftSet) { _ ++ _ }
    }
    
    case NaryOp(_, values) => (values map { listTicVars(b, _, sigma) }).fold(Set()) { _ ++ _ }
  }
  
  private def listSolvedVars(spec: BucketSpec): Set[TicId] = spec match {
    case UnionBucketSpec(left, right) => listSolvedVars(left) ++ listSolvedVars(right)
    case IntersectBucketSpec(left, right) => listSolvedVars(left) ++ listSolvedVars(right)
    case Group(_, _, forest, _) => listSolvedVars(forest)
    case UnfixedSolution(id, _, _) => Set(id)
    case FixedSolution(id, _, _, _) => Set(id)
    case Extra(_, _) => Set()
  }

  private def listSolutionExprs(spec: BucketSpec): Set[Expr] = spec match {
    case UnionBucketSpec(left, right) => listSolutionExprs(left) ++ listSolutionExprs(right)
    case IntersectBucketSpec(left, right) => listSolutionExprs(left) ++ listSolutionExprs(right)
    case Group(_, _, forest, _) => listSolutionExprs(forest)
    case UnfixedSolution(_, expr, _) => Set(expr)
    case FixedSolution(_, solution, _, _) => Set(solution)
    case Extra(expr, _) => Set(expr)
  }
  
  private def findCommonality(solve: Solve, nodes: Set[Expr], sigma: Sigma): Option[Expr] = {
    case class Kernel(nodes: Set[ExprWrapper], sigma: Sigma, seen: Set[ExprWrapper])
    
    @tailrec
    def bfs(kernels: Set[Kernel]): Set[ExprWrapper] = {
      // check for convergence
      val results = kernels flatMap { k =>
        val results = kernels.foldLeft(k.nodes) { _ & _.seen }
        
        // TODO if the below isEmpty, then can drop results from all kernels
        nodes.foldLeft(results) { (results, node) =>
          results filter { ew =>
            listTicVars(Some(solve), ew.expr, k.sigma).isEmpty &&
              isTranspecable(node, ew.expr, k.sigma)
          }
        }
      }
      
      // iterate
      if (results.isEmpty) {
        val kernels2 = kernels map { k =>
          val (nodes2Unflatten, sigma2Unflatten) = k.nodes map { _.expr } map enumerateParents(k.sigma) unzip
          
          val nodes2 = nodes2Unflatten.flatten map ExprWrapper
          val sigma2: Sigma = Map(sigma2Unflatten.flatten.toSeq: _*)
          val nodes3 = nodes2 &~ k.seen
          
          Kernel(nodes3, sigma2, k.seen ++ nodes3)
        }
        
        if (kernels2 forall { _.nodes.isEmpty }) {
          Set()
        } else {
          bfs(kernels2)
        }
      } else {
        results
      }
    }
    
    if (nodes.size == 1) {
      nodes.headOption
    } else {
      val kernels = nodes map { n => Kernel(Set(ExprWrapper(n)), sigma, Set(ExprWrapper(n))) }
      val results = bfs(kernels)
      
      if (results.size == 1)
        results.headOption map { _.expr }
      else
        None
    }
  }
  
  private def enumerateParents(sigma: Sigma)(expr: Expr): (Set[Expr], Sigma) = expr match {
    case Let(_, _, _, _, right) => (Set(right), sigma)
    
    case _: Solve => (Set(), sigma)      // TODO will this do the right thing?
    
    case Assert(_, pred, child) => (Set(pred, child), sigma)
    
    case Observe(_, data, samples) => (Set(data, samples), sigma)
    
    case New(_, child) => (Set(child), sigma)
    
    case Relate(_, _, _, in) => (Set(in), sigma)
    
    case _: TicVar => (Set(), sigma)
    
    case expr @ Dispatch(_, id, actuals) => {
      expr.binding match {
        case LetBinding(let) =>
          (Set(let.left), enterLet(sigma, let, actuals))
        
        case FormalBinding(let) => (Set(sigma((id, let))), sigma)
        
        case _ => (Set(actuals: _*), sigma)
      }
    }
    
    case NaryOp(_, values) => (Set(values: _*), sigma)
  }
  

  sealed trait BucketSpec {
    import buckets._
    
    final def derive(id: TicId, expr: Expr): BucketSpec = this match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(left.derive(id, expr), right.derive(id, expr))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(left.derive(id, expr), right.derive(id, expr))
      
      case Group(origin, target, forest, btrace) =>
        Group(origin, target, forest.derive(id, expr), btrace)
      
      case UnfixedSolution(`id`, solution, dtrace) =>
        FixedSolution(id, solution, expr, dtrace)
      
      case s @ UnfixedSolution(_, _, _) => s
      
      case f @ FixedSolution(id2, _, _, _) => {
        assert(id != id2)
        f
      }
      
      case e @ Extra(_, _) => e
    }
    
    final def exprs: Set[Expr] = this match {
      case UnionBucketSpec(left, right) => left.exprs ++ right.exprs
      case IntersectBucketSpec(left, right) => left.exprs ++ right.exprs
      case Group(_, target, forest, _) => forest.exprs + target
      case UnfixedSolution(_, solution, _) => Set(solution)
      case FixedSolution(_, solution, expr, _) => Set(solution, expr)
      case Extra(expr, _) => Set(expr)
    }
  }
  
  object buckets {
    case class UnionBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    case class IntersectBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    
    case class Group(origin: Option[Where], target: Expr, forest: BucketSpec, btrace: List[Dispatch]) extends BucketSpec
    
    case class UnfixedSolution(id: TicId, solution: Expr, btrace: List[Dispatch]) extends BucketSpec
    case class FixedSolution(id: TicId, solution: Expr, expr: Expr, btrace: List[Dispatch]) extends BucketSpec
    
    case class Extra(expr: Expr, btrace: List[Dispatch]) extends BucketSpec
  }
}
