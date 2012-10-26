package com.precog.quirrel
package emitter

import scala.annotation.tailrec

trait GroupFinder extends parser.AST with Tracer {
  import ast._
  
  def findGroups(solve: Solve): Set[(Map[Formal, Expr], Where)] = {
    val vars = solve.vars map { findVars(solve, _)(solve.child) } reduceOption { _ ++ _ } getOrElse Set()
    
    val trace = buildTrace(Map())(solve.root)
    
    vars flatMap buildBacktrace(trace) flatMap codrill      // TODO minimize by sigma subsetting
  }
  
  private def codrill(btrace: List[(Map[Formal, Expr], Expr)]): Option[(Map[Formal, Expr], Where)] = {
    @tailrec
    def state1(btrace: List[(Map[Formal, Expr], Expr)]): Option[(Map[Formal, Expr], Where)] = btrace match {
      case (_, _: Add | _: Sub | _: Mul | _: Div | _: Neg | _: Paren) :: tail => state1(tail)
      case (_, _: RelationExpr) :: tail => state2(tail)
      case _ => None
    }
    
    @tailrec
    def state2(btrace: List[(Map[Formal, Expr], Expr)]): Option[(Map[Formal, Expr], Where)] = btrace match {
      case (_, _: Comp | _: And | _: Or) :: tail => state2(tail)
      case (sigma, where: Where) :: _ => Some((sigma, where))
      case _ => None
    }
    
    state1(btrace)
  }
  
  private def findVars(solve: Solve, id: TicId)(expr: Expr): Set[TicVar] = expr match {
    case Let(_, _, _, left, right) =>
      findVars(solve, id)(left) ++ findVars(solve, id)(right)
    
    case Solve(_, constraints, child) => {
      val constrVars = constraints map findVars(solve, id) reduceOption { _ ++ _ } getOrElse Set()
      constrVars ++ findVars(solve, id)(child)
    }
    
    case Import(_, _, child) => findVars(solve, id)(child)
    case New(_, child) => findVars(solve, id)(child)
    
    case Relate(_, from, to, in) =>
      findVars(solve, id)(from) ++ findVars(solve, id)(to) ++ findVars(solve, id)(in)
    
    case expr @ TicVar(_, `id`) if expr.binding == SolveBinding(solve) =>
      Set(expr)
    
    case _: TicVar | _: StrLit | _: NumLit | _: BoolLit | _: NullLit => Set()
    
    case ObjectDef(_, props) =>
      props map { _._2 } map findVars(solve, id) reduceOption { _ ++ _ } getOrElse Set()
    
    case ArrayDef(_, values) =>
      values map findVars(solve, id) reduceOption { _ ++ _ } getOrElse Set()
    
    case Descent(_, child, _) => findVars(solve, id)(child)
    
    case Deref(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    
    case Dispatch(_, _, actuals) =>
      actuals map findVars(solve, id) reduceOption { _ ++ _ } getOrElse Set()
    
    case Where(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case With(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Union(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Intersect(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Difference(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    
    case Add(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Sub(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Mul(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Div(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Mod(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    
    case Lt(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case LtEq(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Gt(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case GtEq(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    
    case Eq(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case NotEq(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    
    case And(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    case Or(_, left, right) => findVars(solve, id)(left) ++ findVars(solve, id)(right)
    
    case Comp(_, child) => findVars(solve, id)(child)
    case Neg(_, child) => findVars(solve, id)(child)
    case Paren(_, child) => findVars(solve, id)(child)
  }
}
