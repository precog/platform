package com.precog
package quirrel
package typer

import bytecode.Library

trait Binder extends parser.AST with Library {
  import ast._

  object BuiltIns {
    val Count   = BuiltIn(Identifier(Vector(), "count"), 1, true)
    val Load    = BuiltIn(Identifier(Vector(), "load"), 1, false)
    val Max     = BuiltIn(Identifier(Vector(), "max"), 1, true)
    val Mean    = BuiltIn(Identifier(Vector(), "mean"), 1, true)
    val Median  = BuiltIn(Identifier(Vector(), "median"), 1, true)
    val Min     = BuiltIn(Identifier(Vector(), "min"), 1, true)
    val Mode    = BuiltIn(Identifier(Vector(), "mode"), 1, true)
    val StdDev  = BuiltIn(Identifier(Vector(), "stdDev"), 1, true)
    val Sum     = BuiltIn(Identifier(Vector(), "sum"), 1, true)

    val all = Set(Count, Load, Max, Mean, Median, Min, Mode, StdDev, Sum)
  }

  override def bindNames(tree: Expr) = {
    def loop(tree: Expr, env: Map[Either[TicId, Identifier], Binding]): Set[Error] = tree match {
      case b @ Let(_, id, formals, left, right) => {
        val env2 = formals.foldLeft(env) { (m, s) => m + (Left(s) -> UserDef(b)) }
        loop(left, env2) ++ loop(right, env + (Right(id) -> UserDef(b)))
      }
      
      case New(_, child) => loop(child, env)
      
      case Relate(_, from, to, in) =>
        loop(from, env) ++ loop(to, env) ++ loop(in, env)
      
      case t @ TicVar(_, name) => {
        env get Left(name) match {
          case Some(b @ UserDef(_)) => {
            t.binding = b
            Set()
          }
          
          case None => {
            t.binding = NullBinding
            Set(Error(t, UndefinedTicVariable(name)))
          }
          
          case _ => throw new AssertionError("Cannot reach this point")
        }
      }
        
      case StrLit(_, _) => Set()
      
      case NumLit(_, _) => Set()
      
      case BoolLit(_, _) => Set()
      
      case ObjectDef(_, props) => {
        val results = for ((_, e) <- props)
          yield loop(e, env)
        
        results.fold(Set()) { _ ++ _ }
      }
      
      case ArrayDef(_, values) =>
        (values map { loop(_, env) }).fold(Set()) { _ ++ _ }
      
      case Descent(_, child, _) => loop(child, env)
      
      case Deref(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case d @ Dispatch(_, name, actuals) => {
        val recursive = (actuals map { loop(_, env) }).fold(Set()) { _ ++ _ }
        if (env contains Right(name)) {
          d.binding = env(Right(name))
          
          d.isReduction = env(Right(name)) match {
            case BuiltIn(BuiltIns.Load.name, _, _) => false
            case BuiltIn(_, _, true) => true
            case BuiltIn(_, _, false) => false
            case _ => false
          }
          
          recursive
        } else {
          d.binding = NullBinding
          d.isReduction = false
          recursive + Error(d, UndefinedFunction(name))
        }
      }
      
      case Operation(_, left, _, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Add(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Sub(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Mul(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Div(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Lt(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case LtEq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Gt(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case GtEq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Eq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case NotEq(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case And(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Or(_, left, right) =>
        loop(left, env) ++ loop(right, env)
     
      case Comp(_, child) => loop(child, env)
      
      case Neg(_, child) => loop(child, env)
      
      case Paren(_, child) => loop(child, env)
    }

    loop(tree, (mathlib1.map(StdlibBuiltIn1) ++ mathlib2.map(StdlibBuiltIn2) ++ lib1.map(StdlibBuiltIn1) ++ lib2.map(StdlibBuiltIn2) ++ BuiltIns.all).map({ b => Right(b.name) -> b})(collection.breakOut))
  } 

  sealed trait Binding
  sealed trait FormalBinding extends Binding
  sealed trait FunctionBinding extends Binding {
    def name: Identifier
  }

  // TODO arity and types
  case class BuiltIn(name: Identifier, arity: Int, reduction: Boolean) extends FunctionBinding {
    override val toString = "<native: %s(%d)>".format(name, arity)
  }

  case class StdlibBuiltIn1(f: BIF1) extends FunctionBinding {
    val name = Identifier(f.namespace, f.name)
    override val toString = "<native: %s(%d)>".format(f.name, 1)
  }
  
  case class StdlibBuiltIn2(f: BIF2) extends FunctionBinding {
    val name = Identifier(f.namespace, f.name)
    override val toString = "<native: %s(%d)>".format(f.name, 2)
  }
  
  case class UserDef(b: Let) extends Binding with FormalBinding {
    override val toString = "@%d".format(b.nodeId)
  }
  
  case object NullBinding extends Binding with FormalBinding {
    override val toString = "<null>"
  }
}
