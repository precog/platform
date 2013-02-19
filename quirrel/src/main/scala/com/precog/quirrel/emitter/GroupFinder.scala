package com.precog.quirrel
package emitter

import scala.annotation.tailrec

trait GroupFinder extends parser.AST with Tracer {
  import ast._
  
  def findGroups(solve: Solve): Set[(Map[Formal, Expr], Where, List[Dispatch])] = {
    val vars0 = solve.vars map { findVars(solve, _)(solve.child) } reduceOption { _ ++ _ } getOrElse Set()
    val vars = vars0 map { Some(_) }
    
    // TODO minimize by sigma subsetting
    vars flatMap buildBacktrace(solve.trace) flatMap { btrace =>
      val result = codrill(btrace)
      
      result map {
        case (sigma, where) =>
          val dtrace = btrace map { _._2 } dropWhile { !_.isInstanceOf[Where] } collect {
            case d: Dispatch if d.binding.isInstanceOf[LetBinding] => d
          }
          
          (sigma, where, dtrace)
      }
    }
  }
  
  private def codrill(btrace: List[(Map[Formal, Expr], Expr)]): Option[(Map[Formal, Expr], Where)] = {
    @tailrec
    def state1(btrace: List[(Map[Formal, Expr], Expr)]): Option[(Map[Formal, Expr], Where)] = btrace match {
      case (_, _: Add | _: Sub | _: Mul | _: Div | _: Neg | _: Paren) :: tail => state1(tail)
      case (_, _: ComparisonOp) :: tail => state2(tail)
      case (_, _: Dispatch) :: tail => state1(tail)
      case _ => None
    }
    
    @tailrec
    def state2(btrace: List[(Map[Formal, Expr], Expr)]): Option[(Map[Formal, Expr], Where)] = btrace match {
      case (_, _: Comp | _: And | _: Or) :: tail => state2(tail)
      case (sigma, where: Where) :: _ => Some((sigma, where))
      case (_, _: Dispatch) :: tail => state2(tail)
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
    
    case New(_, child) => findVars(solve, id)(child)
    
    case Relate(_, from, to, in) =>
      findVars(solve, id)(from) ++ findVars(solve, id)(to) ++ findVars(solve, id)(in)
    
    case expr @ TicVar(_, `id`) if expr.binding == SolveBinding(solve) =>
      Set(expr)
    
    case _: TicVar => Set()
    
    case NaryOp(_, values) =>
      values map findVars(solve, id) reduceOption { _ ++ _ } getOrElse Set()
  }
}
