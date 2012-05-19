package com.precog
package quirrel
package typer

import bytecode.Library

trait Binder extends parser.AST with Library {
  import ast._
  
  protected override lazy val LoadId = Identifier(Vector(), "load")

  object BuiltIns {
    val Count          = BuiltIn(Identifier(Vector(), "count"), 1, true)
    val GeometricMean  = BuiltIn(Identifier(Vector(), "geometricMean"), 1, true)
    val Load           = BuiltIn(LoadId, 1, false)
    val Max            = BuiltIn(Identifier(Vector(), "max"), 1, true)
    val Mean           = BuiltIn(Identifier(Vector(), "mean"), 1, true)
    val Median         = BuiltIn(Identifier(Vector(), "median"), 1, true)
    val Min            = BuiltIn(Identifier(Vector(), "min"), 1, true)
    val Mode           = BuiltIn(Identifier(Vector(), "mode"), 1, true)
    val StdDev         = BuiltIn(Identifier(Vector(), "stdDev"), 1, true)
    val Sum            = BuiltIn(Identifier(Vector(), "sum"), 1, true)
    val SumSq          = BuiltIn(Identifier(Vector(), "sumSq"), 1, true)
    val Variance       = BuiltIn(Identifier(Vector(), "variance"), 1, true)

    val Distinct       = BuiltIn(Identifier(Vector(), "distinct"), 1, false)

    val all = Set(Count, GeometricMean, Load, Max, Mean, Median, Min, Mode, StdDev, Sum, SumSq, Variance, Distinct)
  }

  override def bindNames(tree: Expr) = {
    def loop(tree: Expr, env: Map[Either[TicId, Identifier], Binding]): Set[Error] = tree match {
      case b @ Let(_, id, formals, left, right) => {
        val (_, dups) = formals.foldLeft((Set[TicId](), Set[TicId]())) {
          case ((acc, dup), id) if acc(id) => (acc, dup + id)
          case ((acc, dup), id) if !acc(id) => (acc + id, dup)
        }
        
        if (!dups.isEmpty) {
          dups map { id => Error(b, MultiplyDefinedTicVariable(id)) }
        } else {
          val env2 = formals.foldLeft(env) { (m, s) => m + (Left(s) -> UserDef(b)) }
          loop(left, env2) ++ loop(right, env + (Right(id) -> UserDef(b)))
        }
      }
      
      case Import(_, spec, child) => {
        val addend = spec match {
          case SpecificImport(prefix) => {
            env flatMap {
              case (Right(Identifier(ns, name)), b) => {
                if (ns.length >= prefix.length) {
                  if (ns zip prefix forall { case (a, b) => a == b })
                    Some(Right(Identifier(ns drop prefix.length, name)) -> b)
                  else
                    None
                } else if (ns.length == prefix.length - 1) {
                  if (ns zip prefix forall { case (a, b) => a == b }) {
                    if (name == prefix.last)
                      Some(Right(Identifier(Vector(), name)) -> b)
                    else
                      None
                  } else {
                    None
                  }
                } else {
                  None
                }
              }
              
              case _ => None
            }
          }
          
          case WildcardImport(prefix) => {
            env flatMap {
              case (Right(Identifier(ns, name)), b) => {
                if (ns.length >= prefix.length + 1) {
                  if (ns zip prefix forall { case (a, b) => a == b })
                    Some(Right(Identifier(ns drop (prefix.length + 1), name)) -> b)
                  else
                    None
                } else if (ns.length == prefix.length) {
                  if (ns zip prefix forall { case (a, b) => a == b })
                    Some(Right(Identifier(Vector(), name)) -> b)
                  else
                    None
                } else {
                  None
                }
              }
              
              case _ => None
            }
          }
        }
        
        loop(child, env ++ addend)
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

      case NullLit(_) => Set()
      
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
      
      case Where(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case With(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Union(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Intersect(_, left, right) =>
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

    loop(tree, (lib1.map(StdlibBuiltIn1) ++ lib2.map(StdlibBuiltIn2) ++ BuiltIns.all).map({ b => Right(b.name) -> b})(collection.breakOut))
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
