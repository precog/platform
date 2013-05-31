package com.precog.quirrel
package emitter

import scala.annotation.tailrec

trait GroupFinder extends parser.AST with Tracer {
  import ast._
  
  /**
   * Find all groups for a particular solve using the whole world assumption.
   * Any groups with a dtrace which is disjoint with `dispatches` (or even just
   * a strict subset) will be filtered out.  Supersetting is allowed.
   */
  def findGroups(solve: Solve, dispatches: Set[Dispatch]): Set[(Sigma, Where, List[Dispatch])] = {
    val vars = solve.vars map { findVars(solve, _)(solve.child) } reduceOption { _ ++ _ } getOrElse Set()
    
    // TODO minimize by sigma subsetting
    val btraces = vars flatMap buildBacktrace(solve.trace)

    btraces flatMap { btrace =>
      val result = codrill(btrace)
      
      val mapped = result map {
        case (sigma, where) =>
          val dtrace = btrace map { _._2 } dropWhile { !_.isInstanceOf[Where] } collect {
            case d: Dispatch if d.binding.isInstanceOf[LetBinding] => d
          }
          
          (sigma, where, dtrace)
      }
      
      /* 
       * Only include groups which match the set of dispatches.
       * 
       * FIXME this will cause some queries to break erroneously:
       * 
       * foo(x) := solve 'a count(foo where foo = 'a) + x
       * solve 'b count(foo(4) where foo(4) = 'b)
       * 
       * The above is fine, but the actuals length trick will fail to
       * allow it through and the compiler will fail to solve 'a.  Leaving
       * this case unresolve for now since macros are going away.
       */
      mapped filter {
        case (_, _, dtrace) =>
          dispatches filter { _.actuals.length > 0 } forall dtrace.contains
      }
    }
  }
  
  private def codrill(btrace: List[(Sigma, Expr)]): Option[(Sigma, Where)] = {
    @tailrec
    def state1(btrace: List[(Sigma, Expr)]): Option[(Sigma, Where)] = btrace match {
      case (_, _: Add | _: Sub | _: Mul | _: Div | _: Neg | _: Paren) :: tail => state1(tail)
      case (_, _: ComparisonOp) :: tail => state2(tail)
      case (_, _: Dispatch) :: tail => state1(tail)
      case _ => None
    }
    
    @tailrec
    def state2(btrace: List[(Sigma, Expr)]): Option[(Sigma, Where)] = btrace match {
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
    
    case Assert(_, pred, child) =>
      findVars(solve, id)(pred) ++ findVars(solve, id)(child)
    
    case Relate(_, from, to, in) =>
      findVars(solve, id)(from) ++ findVars(solve, id)(to) ++ findVars(solve, id)(in)
    
    case expr @ TicVar(_, `id`) if expr.binding == SolveBinding(solve) =>
      Set(expr)
    
    case _: TicVar => Set()
    
    case NaryOp(_, values) =>
      values map findVars(solve, id) reduceOption { _ ++ _ } getOrElse Set()
  }
}
