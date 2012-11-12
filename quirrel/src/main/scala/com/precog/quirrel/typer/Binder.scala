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
package com.precog
package quirrel
package typer

import bytecode.Library

trait Binder extends parser.AST with Library {
  import ast._
  
  type Formal = (Identifier, Let)
  
  protected override lazy val LoadId = Identifier(Vector(), "load")
  protected override lazy val DistinctId = Identifier(Vector(), "distinct")

  override def bindNames(tree: Expr) = {
    def loop(tree: Expr, env: Env): Set[Error] = tree match {
      case b @ Let(_, id, formals, left, right) => {
        val (_, dups) = formals.foldLeft((Set[TicId](), Set[TicId]())) {
          case ((acc, dup), id) if acc(id) => (acc, dup + id)
          case ((acc, dup), id) if !acc(id) => (acc + id, dup)
        }
        
        if (!dups.isEmpty) {
          dups map { id => Error(b, MultiplyDefinedTicVariable(id)) }
        } else {
          val ids = formals map { Identifier(Vector(), _) }
          val names2 = ids.foldLeft(env.names) { (m, s) => m + (s -> FormalBinding(b)) }
          val env2 = env.copy(names = names2)
          loop(left, env2) ++ loop(right, env.copy(names = env.names + (id -> LetBinding(b))))
        }
      }

      case b @ Solve(_, constraints, child) => {
        val varVector = constraints map listFreeVars(env)
        
        val errors = if (varVector exists { _.isEmpty })
          Set(Error(b, SolveLackingFreeVariables))
        else
          Set[Error]()
        
        val ids = varVector reduce { _ ++ _ }
        b.vars = ids
        
        val freeBindings = ids map { _ -> FreeBinding(b) }
        val constEnv = env.copy(vars = env.vars ++ freeBindings)
        val constErrors = constraints map { loop(_, constEnv) } reduce { _ ++ _ }
        
        val bindings = ids map { id => id -> SolveBinding(b) }
        loop(child, env.copy(vars = env.vars ++ bindings)) ++ constErrors ++ errors
      }
      
      case Import(_, spec, child) => { //todo see scalaz's Boolean.option
        val addend = spec match {
          case SpecificImport(prefix) => {
            env.names flatMap {
              case (Identifier(ns, name), b) => {
                if (ns.length >= prefix.length) {
                  if (ns zip prefix forall { case (a, b) => a == b })
                    Some(Identifier(ns drop (prefix.length - 1), name) -> b)
                  else
                    None
                } else if (ns.length == prefix.length - 1) {
                  if (ns zip prefix forall { case (a, b) => a == b }) {
                    if (name == prefix.last)
                      Some(Identifier(Vector(), name) -> b) 
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
            env.names flatMap {
              case (Identifier(ns, name), b) => {
                if (ns.length >= prefix.length + 1) {
                  if (ns zip prefix forall { case (a, b) => a == b })
                    Some(Identifier(ns drop prefix.length, name) -> b)
                  else
                    None
                } else if (ns.length == prefix.length) {
                  if (ns zip prefix forall { case (a, b) => a == b })
                    Some(Identifier(Vector(), name) -> b)
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
        
        loop(child, env.copy(names = env.names ++ addend))
      }
      
      case New(_, child) => loop(child, env)
      
      case Relate(_, from, to, in) =>
        loop(from, env) ++ loop(to, env) ++ loop(in, env)
      
      case t @ TicVar(_, name) => {
        env.vars get name match {
          case Some(b) => {
            t.binding = b
            Set()
          }
          case None => {
            t.binding = NullBinding
            Set(Error(t, UndefinedTicVariable(name)))
          }
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
      
      case MetaDescent(_, child, _) => loop(child, env)
      
      case Deref(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case d @ Dispatch(_, name, actuals) => {
        val recursive = (actuals map { loop(_, env) }).fold(Set()) { _ ++ _ }
        if (env.names contains name) {
          val binding = env.names(name)
          
          val arity = binding match {
            case FormalBinding(_) => 0
            case LetBinding(let) => let.params.length
            case ReductionBinding(_) => 1
            case LoadBinding => 1
            case DistinctBinding => 1
            case Morphism1Binding(_) => 1
            case Morphism2Binding(_) => 2
            case Op1Binding(_) => 1
            case Op2Binding(_) => 2
            case NullBinding => sys.error("unreachable code")
          }
          
          val errors = if (actuals.length == arity) {
            d.binding = binding
            Set()
          } else {
            d.binding = NullBinding
            Set(Error(d, IncorrectArity(arity, actuals.length)))
          }
          
          d.isReduction = env.names(name) match {
            case ReductionBinding(_) => true
            case _ => false
          }
          
          binding match {
            case LetBinding(let) =>
              let dispatches_+= d
            
            case _ =>
          }
          
          recursive ++ errors
        } else {
          d.binding = NullBinding
          d.isReduction = false
          recursive + Error(d, UndefinedFunction(name))
        }
      }

      case Cond(_, pred, left, right) =>
        loop(pred, env) ++ loop(left, env) ++ loop(right, env)
      
      case Where(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case With(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Union(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Intersect(_, left, right) =>
        loop(left, env) ++ loop(right, env)
            
      case Difference(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Add(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Sub(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Mul(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Div(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case Mod(_, left, right) =>
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
    
    val builtIns = lib1.map(Op1Binding) ++
      lib2.map(Op2Binding) ++
      libReduction.map(ReductionBinding) ++
      libMorphism1.map(Morphism1Binding) ++
      libMorphism2.map(Morphism2Binding) ++
      Set(LoadBinding, DistinctBinding)
      
    val env = Env(Map(), builtIns.map({ b => b.name -> b })(collection.breakOut))

    loop(tree, env)
  }
  
  private def listFreeVars(env: Env)(expr: Expr): Set[TicId] = expr match {
    case Let(_, _, _, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Solve(_, _, _) => Set()
    case Import(_, _, child) => listFreeVars(env)(child)
    case New(_, child) => listFreeVars(env)(child)
    case Relate(_, from, to, in) => listFreeVars(env)(from) ++ listFreeVars(env)(to) ++ listFreeVars(env)(in)
    case TicVar(_, name) if env.vars contains name => Set()
    case TicVar(_, name) if !(env.vars contains name) => Set(name)
    case StrLit(_, _) => Set()
    case NumLit(_, _) => Set()
    case BoolLit(_, _) => Set()
    case NullLit(_) => Set()
    case ObjectDef(_, props) => props map { _._2 } map listFreeVars(env) reduceOption { _ ++ _ } getOrElse Set()
    case ArrayDef(_, values) => values map listFreeVars(env) reduceOption { _ ++ _ } getOrElse Set()
    case Descent(_, child, _) => listFreeVars(env)(child)
    case Dispatch(_, _, actuals) => actuals map listFreeVars(env) reduceOption { _ ++ _ } getOrElse Set()
    case Where(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case With(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Union(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Intersect(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Difference(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Add(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Sub(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Mul(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Div(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Mod(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Lt(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case LtEq(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Gt(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case GtEq(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Eq(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case NotEq(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case And(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Or(_, left, right) => listFreeVars(env)(left) ++ listFreeVars(env)(right)
    case Comp(_, child) => listFreeVars(env)(child)
    case Neg(_, child) => listFreeVars(env)(child)
    case Paren(_, child) => listFreeVars(env)(child)
  }
  
  
  private case class Env(vars: Map[TicId, VarBinding], names: Map[Identifier, NameBinding])

  sealed trait NameBinding
  sealed trait VarBinding
  
  case class LetBinding(b: Let) extends NameBinding {
    override val toString = "@%d".format(b.nodeId)
  }
  
  case class FormalBinding(b: Let) extends NameBinding {
    override val toString = "@%d".format(b.nodeId)
  }
  
  sealed trait BuiltInBinding extends NameBinding {
    def name: Identifier
  }
  
  // TODO arity and types
  case class ReductionBinding(red: Reduction) extends BuiltInBinding {
    val name = Identifier(red.namespace, red.name)
    override val toString = "<native: %s(%d)>".format(red.name, 1)   //assumes all reductions are arity 1
  }  
  
  case object DistinctBinding extends BuiltInBinding {
    val name = DistinctId
    override val toString = "<native: distinct(1)>"
  }  

  case object LoadBinding extends BuiltInBinding {
    val name = LoadId
    override val toString = "<native: load(1)>"
  }

  case class Morphism1Binding(mor: Morphism1) extends BuiltInBinding {
    val name = Identifier(mor.namespace, mor.name)
    override val toString = "<native: %s(%d)>".format(mor.name, 1)
  }

  case class Morphism2Binding(mor: Morphism2) extends BuiltInBinding {
    val name = Identifier(mor.namespace, mor.name)
    override val toString = "<native: %s(%d)>".format(mor.name, 2)
  }

  case class Op1Binding(op1: Op1) extends BuiltInBinding {
    val name = Identifier(op1.namespace, op1.name)
    override val toString = "<native: %s(%d)>".format(op1.name, 1)
  }
  
  case class Op2Binding(op2: Op2) extends BuiltInBinding {
    val name = Identifier(op2.namespace, op2.name)
    override val toString = "<native: %s(%d)>".format(op2.name, 2)
  }
  
  case class SolveBinding(solve: Solve) extends VarBinding {
    override val toString = "@%d".format(solve.nodeId)
  }
  
  case class FreeBinding(solve: Solve) extends VarBinding {
    override val toString = "@%d".format(solve.nodeId)
  }

  case object NullBinding extends NameBinding with VarBinding {
    override val toString = "<null>"
  }
}
