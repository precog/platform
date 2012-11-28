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

import com.codecommit.gll.ast.Node

import parser._
import typer._

trait GroupSolver extends AST with GroupFinder with Solver with ProvenanceChecker {
  import Function._
  
  import ast._
  import buckets._
  
  override def inferBuckets(tree: Expr): Set[Error] = {
    def loop(dispatches: Set[Dispatch])(tree: Expr): Set[Error] = tree match {
      case Let(_, _, _, _, right) => loop(dispatches)(right)
      
      case expr @ Solve(_, constraints, child) => {
        val constrLoopErrors = constraints map loop(dispatches) reduce { _ ++ _ }
        val childErrors = loop(dispatches)(child)
        
        val sigma: Map[Formal, Expr] = dispatches flatMap { dispatch =>
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
        
        val (forestSpecM, forestErrors) = solveForest(expr, findGroups(expr))
        
        val filtered = constraints filter {
          case TicVar(_, _) => false
          case _ => true
        }
        
        val (constrSpecM, constrErrors) = mergeSpecs(filtered map { solveConstraint(expr, _, sigma) })
        
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
        
        specM foreach { spec =>
          expr buckets_+= (dispatches -> spec)
        }
        
        val forestErrors2 = if (finalErrors.isEmpty) {
          forestErrors filter {
            case Error(tpe) => tpe == ConstraintsWithinInnerSolve
          }
        } else {
          forestErrors
        }
        
        constrLoopErrors ++ childErrors ++ forestErrors2 ++ constrErrors ++ finalErrors
      }
      
      case Import(_, _, child) => loop(dispatches)(child)
      
      case New(_, child) => loop(dispatches)(child)
      
      case Relate(_, from, to, in) =>
        loop(dispatches)(from) ++ loop(dispatches)(to) ++ loop(dispatches)(in)
      
      case TicVar(_, _) => Set()
      case StrLit(_, _) => Set()
      case NumLit(_, _) => Set()
      case BoolLit(_, _) => Set()
      case NullLit(_) => Set()
      
      case ObjectDef(_, props) =>
        (props map { case (_, e) => loop(dispatches)(e) }).fold(Set[Error]()) { _ ++ _ }
      
      case ArrayDef(_, values) =>
        (values map loop(dispatches)).fold(Set[Error]()) { _ ++ _ }
      
      case Descent(_, child, _) => loop(dispatches)(child)
      
      case MetaDescent(_, child, _) => loop(dispatches)(child)
      
      case Deref(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case d @ Dispatch(_, _, actuals) => {
        val actualErrors = (actuals map loop(dispatches)).fold(Set[Error]()) { _ ++ _ }
        
        val originErrors = d.binding match {
          case LetBinding(let) => loop(dispatches + d)(let.left)
          case _ => Set()
        }
        
        actualErrors ++ originErrors
      }

      case Cond(_, pred, left, right) =>
        loop(dispatches)(pred) ++ loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Where(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case With(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Union(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Intersect(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Difference(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Add(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Sub(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Mul(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Div(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Mod(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Lt(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case LtEq(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Gt(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case GtEq(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Eq(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case NotEq(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case And(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Or(_, left, right) =>
        loop(dispatches)(left) ++ loop(dispatches)(right)
      
      case Comp(_, child) => loop(dispatches)(child)
      
      case Neg(_, child) => loop(dispatches)(child)
      
      case Paren(_, child) => loop(dispatches)(child)
    }
    
    loop(Set())(tree)
  }
  
  private def solveForest(solve: Solve, forest: Set[(Map[Formal, Expr], Where, List[Dispatch])]): (Option[BucketSpec], Set[Error]) = {
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
        
        val commonalityM = findCommonality(Set(where.left, where.right), sigma, orderedSigma)
        
        val fullyResolvedLeftProv = resolveUnifications(where.left.relations)(resolvedLeftProv.makeCanonical)
        val fullyResolvedRightProv = resolveUnifications(where.right.relations)(resolvedRightProv.makeCanonical)
        
        commonalityM filter const(fullyResolvedLeftProv == fullyResolvedRightProv) map { commonality =>
          // 1. test where.left isTranspecable
          // 2. attempt to solve where.right
          
          if (isTranspecableFrom(where.left, commonality, sigma)) {
            val (group, errors) = solveGroupCondition(solve, where.right, false, sigma)
            (group map { Group(Some(where), resolveExpr(sigma, where.left), _, dtrace) }, errors)
          } else {
            (None, Set[Error]())      // TODO when we implement isTranspecable
          }
        } getOrElse (None, Set[Error]())
      }
    }
    
    mergeSpecs(results)
  }
  
  private def resolveExpr(sigma: Map[Formal, Expr], expr: Expr): Expr = expr match {
    case expr @ Dispatch(_, id, _) => {
      expr.binding match {
        case FormalBinding(let) => resolveExpr(sigma, sigma((id, let)))
        case _ => expr
      }
    }
    
    case _ => expr
  }
  
  private def solveConstraint(b: Solve, constraint: Expr, sigma: Map[Formal, Expr]): (Option[BucketSpec], Set[Error]) = {
    val (result, errors) = solveGroupCondition(b, constraint, true, sigma)
    
    val orderedSigma = orderTopologically(sigma)
    val commonality = result map listSolutionExprs flatMap { findCommonality(_, sigma, orderedSigma) }
    
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
  
  private def orderTopologically(sigma: Map[Formal, Expr]): List[Formal] = {
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
  
  private def solveGroupCondition(b: Solve, expr: Expr, free: Boolean, sigma: Map[Formal, Expr]): (Option[BucketSpec], Set[Error]) = expr match {
    case And(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left, free, sigma)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right, free, sigma)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield IntersectBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case Or(_, left, right) => {
      val (leftSpec, leftErrors) = solveGroupCondition(b, left, free, sigma)
      val (rightSpec, rightErrors) = solveGroupCondition(b, right, free, sigma)
      
      val andSpec = for (ls <- leftSpec; rs <- rightSpec)
        yield UnionBucketSpec(ls, rs)
      
      (andSpec orElse leftSpec orElse rightSpec, leftErrors ++ rightErrors)
    }
    
    case expr: RelationExpr if !listTicVars(Some(b), expr, sigma).isEmpty => {
      val vars = listTicVars(Some(b), expr, sigma)
      
      if (vars.size > 1) {
        val ticVars = vars map { case (_, id) => id }
        (None, Set(Error(expr, InseparablePairedTicVariables(ticVars))))
      } else {
        val tv = vars.head._2
        val result = solveRelation(expr, sigma)(pred(b, tv, free, sigma))
        
        if (result.isDefined)
          (result map { UnfixedSolution(tv, _) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
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
          (result map { UnfixedSolution(tv, _) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }
    
    case expr @ Dispatch(_, id, actuals) => {
      expr.binding match {
        case LetBinding(let) => {
          val ids = let.params map { Identifier(Vector(), _) }
          val sigma2 = sigma ++ (ids zip Stream.continually(let) zip actuals)
          solveGroupCondition(b, let.left, free, sigma2)
        }
        
        case FormalBinding(let) => {
          val actualM = sigma get ((id, let))
          val resultM = actualM map { solveGroupCondition(b, _, free, sigma) }
          resultM getOrElse sys.error("uh...?")
        }
      }
    }

    case expr if listTicVars(Some(b), expr, sigma).isEmpty && !listTicVars(None, expr, sigma).isEmpty =>
      (None, Set(Error(expr, ConstraintsWithinInnerSolve)))
    
    case _ if listTicVars(Some(b), expr, sigma).isEmpty => 
      (Some(Extra(expr)), Set())
    
    case _ => (None, listTicVars(Some(b), expr, sigma) map { case (_, id) => id } map UnableToSolveTicVariable map { Error(expr, _) })
  }
  
  // my appologies to humanity...
  private def pred(b: Solve, tv: TicId, free: Boolean, sigma: Map[Formal, Expr]): PartialFunction[Node, Boolean] = {
    case d @ Dispatch(_, id, actuals) => {
      d.binding match {
        case FormalBinding(let) => {
          val actualM = sigma get ((id, let))
          actualM flatMap pred(b, tv, free, sigma).lift getOrElse false
        }
        
        case LetBinding(let) => {
          val ids = let.params map { Identifier(Vector(), _) }
          val sigma2 = sigma ++ (ids zip Stream.continually(let) zip actuals)
          
          pred(b, tv, free, sigma2).lift(let.left) getOrElse false
        }
        
        case _ => false
      }
    }
    
    case t @ TicVar(_, `tv`) => !free && t.binding == SolveBinding(b) || free && t.binding == FreeBinding(b)
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
  
  private def isTranspecableFrom(to: Expr, from: Expr, sigma: Map[Formal, Expr]): Boolean = 
    true      // TODO blame alissa
  
  //if b is Some: finds all tic vars in the Expr that have the given Solve as their binding
  //if b is None: finds all tic vars in the Expr
  private def listTicVars(b: Option[Solve], expr: Expr, sigma: Map[Formal, Expr]): Set[(Option[Solve], TicId)] = expr match {
    case Let(_, _, _, left, right) => listTicVars(b, right, sigma)
    
    case b2 @ Solve(_, constraints, child) => {
      val allVars = (constraints map { listTicVars(b, _, sigma) } reduce { _ ++ _ })
      allVars -- listTicVars(Some(b2), child, sigma)
    }
    
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
    case StrLit(_, _) => Set()
    case NumLit(_, _) => Set()
    case BoolLit(_, _) => Set()
    case NullLit(_) => Set()
    case ObjectDef(_, props) => (props.unzip._2 map { listTicVars(b, _, sigma) }).fold(Set()) { _ ++ _ }
    case ArrayDef(_, values) => (values map { listTicVars(b, _, sigma) }).fold(Set()) { _ ++ _ }
    case Descent(_, child, _) => listTicVars(b, child, sigma)
    case MetaDescent(_, child, _) => listTicVars(b, child, sigma)
    case Deref(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    
    case d @ Dispatch(_, id, actuals) => {
      val leftSet = d.binding match {
        case LetBinding(b2) => {
          val ids = b2.params map { Identifier(Vector(), _) }
          val sigma2 = sigma ++ (ids zip Stream.continually(b2) zip actuals)
          listTicVars(b, b2.left, sigma2)
        }
        
        case FormalBinding(let) => listTicVars(b, sigma((id, let)), sigma)
        case _ => Set[(Option[Solve], TicId)]()
      }
      (actuals map { listTicVars(b, _, sigma) }).fold(leftSet) { _ ++ _ }
    }

    case Cond(_, pred, left, right) => listTicVars(b, pred, sigma) ++ listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Where(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case With(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Union(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Intersect(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Add(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Sub(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Mul(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Div(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Mod(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Lt(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case LtEq(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Gt(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case GtEq(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Eq(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case NotEq(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case And(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Or(_, left, right) => listTicVars(b, left, sigma) ++ listTicVars(b, right, sigma)
    case Comp(_, child) => listTicVars(b, child, sigma)
    case Neg(_, child) => listTicVars(b, child, sigma)
    case Paren(_, child) => listTicVars(b, child, sigma)
  }
  
  private def listSolvedVars(spec: BucketSpec): Set[TicId] = spec match {
    case UnionBucketSpec(left, right) => listSolvedVars(left) ++ listSolvedVars(right)
    case IntersectBucketSpec(left, right) => listSolvedVars(left) ++ listSolvedVars(right)
    case Group(_, _, forest, _) => listSolvedVars(forest)
    case UnfixedSolution(id, _) => Set(id)
    case FixedSolution(id, _, _) => Set(id)
    case Extra(_) => Set()
  }

  private def listSolutionExprs(spec: BucketSpec): Set[Expr] = spec match {
    case UnionBucketSpec(left, right) => listSolutionExprs(left) ++ listSolutionExprs(right)
    case IntersectBucketSpec(left, right) => listSolutionExprs(left) ++ listSolutionExprs(right)
    case Group(_, _, forest, _) => listSolutionExprs(forest)
    case UnfixedSolution(_, expr) => Set(expr)
    case FixedSolution(_, solution, _) => Set(solution)
    case Extra(expr) => Set(expr)
  }
  
  
  private def findCommonality(exprs: Set[Expr], sigma: Map[Formal, Expr], order: List[Formal]): Option[Expr] = {
    if (exprs.size <= 1) {
      exprs.headOption
    } else {
      val env: Map[Formal, Set[List[Expr]]] = order.reverse.foldLeft(Map[Formal, Set[List[Expr]]]()) { (env, formal) =>
        val results = buildChains(env)(sigma(formal))
        env + (formal -> results)
      }
          
      val sharedPrefixReversed = exprs flatMap buildChains(env) map { _.reverse } reduceOption { (left, right) =>
        left zip right takeWhile { case (a, b) => a equalsIgnoreLoc b } map { _._1 }
      }
      
      sharedPrefixReversed flatMap { _.lastOption }
    }
  }
    
  private def buildChains(env: Map[(Identifier, Let), Set[List[Expr]]])(expr: Expr): Set[List[Expr]] = expr match {
    case Let(_, _, _, _, right) => buildChains(env)(right) map { expr :: _ }
    
    case expr @ Solve(_, _, _) => Set(expr :: Nil)
    
    case Import(_, _, child) => buildChains(env)(child) map { expr :: _ }
    case New(_, child) => buildChains(env)(child) map { expr :: _ }
    case expr @ Relate(_, _, _, _) => Set(expr :: Nil)
    
    case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => Set()
    
    case ObjectDef(_, props) =>
      props map { _._2 } map buildChains(env) reduceOption { _ ++ _ } getOrElse Set[List[Expr]]() map { expr :: _ }
    
    case ArrayDef(_, values) =>
      values map buildChains(env) reduceOption { _ ++ _ } getOrElse Set[List[Expr]]() map { expr :: _ }
    
    case Descent(_, left, _) =>
      buildChains(env)(left) map { expr :: _ }
    
    case expr @ Dispatch(_, id, actuals) => {
      val actualChains = actuals map buildChains(env)
      
      val dispatchChains = expr.binding match {
        case FormalBinding(let) => env.getOrElse((id, let), Set[List[Expr]]())
        
        case LetBinding(let) => {
          val env2 = env ++ (let.params map { Identifier(Vector(), _) } zip (Stream continually let) zip actualChains)
          buildChains(env2)(let.left)
        }
        
        case ReductionBinding(_) => Set[List[Expr]]()
        
        case Op1Binding(_) | Op2Binding(_) => actualChains reduce { _ ++ _ }
        
        case _ => Set[List[Expr]](Nil)
      }
      
      dispatchChains map { expr :: _ }
    }
    
    case Cond(_, pred, left, right) => 
      (buildChains(env)(pred) ++ buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Where(_, left, right) => 
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case With(_, left, right) => 
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case expr @ (Union(_, _, _) | Intersect(_, _, _) | Difference(_, _, _)) =>
      Set(expr :: Nil)
    
    case Add(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Sub(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Mul(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Div(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Lt(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case LtEq(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Gt(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case GtEq(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Eq(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case NotEq(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case And(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Or(_, left, right) =>
      (buildChains(env)(left) ++ buildChains(env)(right)) map { expr :: _ }
    
    case Comp(_, child) => buildChains(env)(child) map { expr :: _ }
    case Neg(_, child) => buildChains(env)(child) map { expr :: _ }
    case Paren(_, child) => buildChains(env)(child) map { expr :: _ }
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
      
      case UnfixedSolution(`id`, solution) =>
        FixedSolution(id, solution, expr)
      
      case s @ UnfixedSolution(_, _) => s
      
      case f @ FixedSolution(id2, _, _) => {
        assert(id != id2)
        f
      }
      
      case e @ Extra(_) => e
    }
    
    final def exprs: Set[Expr] = this match {
      case UnionBucketSpec(left, right) => left.exprs ++ right.exprs
      case IntersectBucketSpec(left, right) => left.exprs ++ right.exprs
      case Group(_, target, forest, _) => forest.exprs + target
      case UnfixedSolution(_, solution) => Set(solution)
      case FixedSolution(_, solution, expr) => Set(solution, expr)
      case Extra(expr) => Set(expr)
    }
  }
  
  object buckets {
    case class UnionBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    case class IntersectBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    
    case class Group(origin: Option[Where], target: Expr, forest: BucketSpec, btrace: List[Dispatch]) extends BucketSpec
    
    case class UnfixedSolution(id: TicId, solution: Expr) extends BucketSpec
    case class FixedSolution(id: TicId, solution: Expr, expr: Expr) extends BucketSpec
    
    case class Extra(expr: Expr) extends BucketSpec
  }
}
