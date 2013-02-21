package com.precog.quirrel

import scalaz.Tree

trait Tracer extends parser.AST with typer.Binder {
  import ast._
  import Stream.{empty => SNil}
  
  override def buildTrace(sigma: Map[Formal, Expr])(expr: Expr): Tree[(Map[Formal, Expr], Expr)] = expr match {
    case Let(_, _, _, _, right) => buildTrace(sigma)(right)
    
    case Solve(_, constraints, child) => {
      val nodes = buildTrace(sigma)(child) #:: Stream(constraints map buildTrace(sigma): _*)
      Tree.node((sigma, expr), nodes)
    }
    
    case Assert(_, pred, child) =>
      Tree.node((sigma, expr), buildTrace(sigma)(pred) #:: buildTrace(sigma)(child) #:: SNil)
    
    case Observe(_, data, samples) =>
      Tree.node((sigma, expr), buildTrace(sigma)(data) #:: buildTrace(sigma)(samples) #:: SNil)
    
    case New(_, child) =>
      Tree.node((sigma, expr), buildTrace(sigma)(child) #:: SNil)
    
    case Relate(_, from, to, in) =>
      Tree.node((sigma, expr), buildTrace(sigma)(from) #:: buildTrace(sigma)(to) #:: buildTrace(sigma)(in) #:: SNil)
    
    case _: TicVar =>
      Tree.node((sigma, expr), SNil)
    
    case expr @ Dispatch(_, name, actuals) => {
      expr.binding match {
        case LetBinding(let) => {
          val ids = let.params map { Identifier(Vector(), _) }
          val sigma2 = sigma ++ (ids zip Stream.continually(let) zip actuals)
          
          if (actuals.length > 0)
            Tree.node((sigma, expr), buildTrace(sigma2)(let.left) #:: SNil)
          else
            buildTrace(sigma2)(let.left)
        }
        
        case FormalBinding(let) =>
          buildTrace(sigma)(sigma((name, let)))
        
        case _ =>
          Tree.node((sigma, expr), Stream(actuals map buildTrace(sigma): _*))
      }
    }
    
    case NaryOp(_, values) =>
      Tree.node((sigma, expr), Stream(values map buildTrace(sigma): _*))
  }
  
  /**
   * Returns a set of backtraces, where each backtrace is a stack of expressions
   * and associated actual context.
   */
  def buildBacktrace(trace: Tree[(Map[Formal, Expr], Expr)])(target: Expr): Set[List[(Map[Formal, Expr], Expr)]] = {
    def loop(stack: List[(Map[Formal, Expr], Expr)])(trace: Tree[(Map[Formal, Expr], Expr)]): Set[List[(Map[Formal, Expr], Expr)]] = {
      val Tree.Node(pair @ (_, expr), children) = trace

      if (expr == target)
        Set(stack)
      else
        children map loop(pair :: stack) reduceOption { _ ++ _ } getOrElse Set()
    }
    
    loop(Nil)(trace)
  }
}
