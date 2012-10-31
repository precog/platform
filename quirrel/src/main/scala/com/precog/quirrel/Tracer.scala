package com.precog.quirrel

import scalaz.Tree

trait Tracer extends parser.AST with typer.Binder {
  import ast._
  import Stream.{empty => SNil}
  
  def buildTrace(sigma: Map[Formal, Expr])(expr: Expr): Tree[Expr] = expr match {
    case Let(_, _, _, _, right) => buildTrace(sigma)(right)
    
    case Solve(_, constraints, child) => {
      val nodes = buildTrace(sigma)(child) #:: Stream(constraints map buildTrace(sigma): _*)
      Tree.node(expr, nodes)
    }
    
    case Import(_, _, child) => Tree.node(expr, buildTrace(sigma)(child) #:: SNil)
    case New(_, child) => Tree.node(expr, buildTrace(sigma)(child) #:: SNil)
    
    case Relate(_, from, to, in) =>
      Tree.node(expr, buildTrace(sigma)(from) #:: buildTrace(sigma)(to) #:: buildTrace(sigma)(in) #:: SNil)
    
    case _: TicVar | _: StrLit | _: NumLit | _: BoolLit | _: NullLit =>
      Tree.node(expr, SNil)
    
    
    case ObjectDef(_, props) =>
      Tree.node(expr, Stream(props map { _._2 } map buildTrace(sigma) toSeq: _*))
    
    case ArrayDef(_, values) =>
      Tree.node(expr, Stream(values map buildTrace(sigma): _*))
    
    case Descent(_, child, _) =>
      Tree.node(expr, buildTrace(sigma)(child) #:: SNil)
    
    case MetaDescent(_, child, _) =>
      Tree.node(expr, buildTrace(sigma)(child) #:: SNil)
    
    case Deref(_, left, right) =>
      Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    
    case expr @ Dispatch(_, name, actuals) => {
      expr.binding match {
        case LetBinding(let) => {
          val ids = let.params map { Identifier(Vector(), _) }
          val sigma2 = sigma ++ (ids zip Stream.continually(let) zip actuals)
          
          buildTrace(sigma2)(let.left)
        }
        
        case FormalBinding(let) =>
          buildTrace(sigma)(sigma((name, let)))
        
        case _ =>
          Tree.node(expr, Stream(actuals map buildTrace(sigma): _*))
      }
    }
    
    case Where(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case With(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Union(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Intersect(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Difference(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    
    case Add(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Sub(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Mul(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Div(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Mod(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    
    case Lt(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case LtEq(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Gt(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case GtEq(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    
    case Eq(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case NotEq(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    
    case And(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    case Or(_, left, right) => Tree.node(expr, buildTrace(sigma)(left) #:: buildTrace(sigma)(right) #:: SNil)
    
    case Comp(_, child) => Tree.node(expr, buildTrace(sigma)(child) #:: SNil)
    case Neg(_, child) => Tree.node(expr, buildTrace(sigma)(child) #:: SNil)
    case Paren(_, child) => Tree.node(expr, buildTrace(sigma)(child) #:: SNil)
  }
  
  def buildBacktrace(trace: Tree[Expr])(target: Expr): Set[List[Expr]] = {
    def loop(stack: List[Expr])(trace: Tree[Expr]): Set[List[Expr]] = {
      val Tree.Node(expr, children) = trace
      children map loop(expr :: stack) reduceOption { _ ++ _ } getOrElse {
        if (target == expr)
          Set(stack)
        else
          Set()
      }
    }
    
    loop(Nil)(trace)
  }
}
