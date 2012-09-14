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
    val (conditions, reductions) = forest partition {
      case c: GroupCondition => true
      // case r: GroupReduction => false
    }
    
    val (spec, condErrors) = {
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
    
    (spec, condErrors)
  }
  
  private def solveGroupCondition(b: Solve, expr: Expr): (Option[BucketSpec], Set[Error]) = expr match {
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
        val result = solveRelation(expr) { case t @ TicVar(_, `tv`) => t.binding == SolveBinding(b) }
        
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
        val result = solveComplement(expr) { case t @ TicVar(_, `tv`) => t.binding == SolveBinding(b) }
        
        if (result.isDefined)
          (result map { UnfixedSolution(tv, _) }, Set())
        else
          (None, Set(Error(expr, UnableToSolveTicVariable(tv))))
      }
    }
    
    case _ if listTicVars(b, expr).isEmpty => (Some(Extra(expr)), Set())
    
    case _ => (None, listTicVars(b, expr) map UnableToSolveTicVariable map { Error(expr, _) })
  }
  
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
  
  private def listTicVars(b: Solve, expr: Expr): Set[TicId] = expr match {
    case Let(_, _, _, left, right) => listTicVars(b, left) ++ listTicVars(b, right)
    case Solve(_, constraints, child) => (constraints map { listTicVars(b, _) } reduce { _ ++ _ }) ++ listTicVars(b, child)
    case New(_, child) => listTicVars(b, child)
    case Relate(_, from, to, in) => listTicVars(b, from) ++ listTicVars(b, to) ++ listTicVars(b, in)
    case t @ TicVar(_, name) if t.binding == SolveBinding(b) => Set(name)
    case TicVar(_, _) => Set()
    case StrLit(_, _) => Set()
    case NumLit(_, _) => Set()
    case BoolLit(_, _) => Set()
    case NullLit(_) => Set()
    case ObjectDef(_, props) => (props.unzip._2 map { listTicVars(b, _) }).fold(Set()) { _ ++ _ }
    case ArrayDef(_, values) => (values map { listTicVars(b, _) }).fold(Set()) { _ ++ _ }
    case Descent(_, child, _) => listTicVars(b, child)
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
