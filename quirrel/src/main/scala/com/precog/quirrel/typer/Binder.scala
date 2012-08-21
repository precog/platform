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
  
  protected override lazy val LoadId = Identifier(Vector(), "load")
  protected override lazy val DistinctId = Identifier(Vector(), "distinct")

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
          val env2 = formals.foldLeft(env) { (m, s) => m + (Left(s) -> LetBinding(b)) }
          loop(left, env2) ++ loop(right, env + (Right(id) -> LetBinding(b)))
        }
      }

      case b @ Forall(_, param, child) => {
        loop(child, env + (Left(param) -> ForallDef(b)))
      }
      
      case Import(_, spec, child) => { //todo see scalaz's Boolean.option
        val addend = spec match {
          case SpecificImport(prefix) => {
            env flatMap {
              case (Right(Identifier(ns, name)), b) => {
                if (ns.length >= prefix.length) {
                  if (ns zip prefix forall { case (a, b) => a == b })
                    Some(Right(Identifier(ns drop (prefix.length - 1), name)) -> b)
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
                    Some(Right(Identifier(ns drop prefix.length, name)) -> b)
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
          case Some(b @ LetBinding(_)) => {
            t.binding = b
            Set()
          }
          case Some(b @ ForallDef(_)) => {
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
      
      case MetaDescent(_, child, _) => loop(child, env)
      
      case Deref(_, left, right) =>
        loop(left, env) ++ loop(right, env)
      
      case d @ Dispatch(_, name, actuals) => {
        val recursive = (actuals map { loop(_, env) }).fold(Set()) { _ ++ _ }
        if (env contains Right(name)) {
          d.binding = env(Right(name))
          
          d.isReduction = env(Right(name)) match {
            case ReductionBinding(_) => true
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

    loop(tree, (lib1.map(Op1Binding) ++ lib2.map(Op2Binding) ++ libReduction.map(ReductionBinding) ++ libMorphism1.map(Morphism1Binding) ++ libMorphism2.map(Morphism2Binding) ++ Set(LoadBinding(LoadId), DistinctBinding(DistinctId))).map({ b => Right(b.name) -> b})(collection.breakOut))
  } 

  sealed trait Binding
  sealed trait FormalBinding extends Binding
  sealed trait FunctionBinding extends Binding {
    def name: Identifier
  }

  // TODO arity and types
  case class ReductionBinding(red: Reduction) extends FunctionBinding {
    val name = Identifier(red.namespace, red.name)
    override val toString = "<native: %s(%d)>".format(red.name, 1)   //assumes all reductions are arity 1
  }  
  
  case class DistinctBinding(id: Identifier) extends FunctionBinding {  //TODO do we need the `id` parameter? for `name`?
    val name = Identifier(id.namespace, id.id)
    override val toString = "<native: %s(%d)>".format(id.id, 1)
  }  

  case class LoadBinding(id: Identifier) extends FunctionBinding {  //TODO do we need the `id` parameter? for `name`?
    val name = Identifier(id.namespace, id.id)
    override val toString = "<native: %s(%d)>".format(id.id, 1)
  }

  case class Morphism1Binding(mor: Morphism1) extends FunctionBinding {
    val name = Identifier(mor.namespace, mor.name)
    override val toString = "<native: %s(%d)>".format(mor.name, 1)
  }

  case class Morphism2Binding(mor: Morphism2) extends FunctionBinding {
    val name = Identifier(mor.namespace, mor.name)
    override val toString = "<native: %s(%d)>".format(mor.name, 1)
  }

  case class Op1Binding(op1: Op1) extends FunctionBinding {
    val name = Identifier(op1.namespace, op1.name)
    override val toString = "<native: %s(%d)>".format(op1.name, 1)
  }
  
  case class Op2Binding(op2: Op2) extends FunctionBinding {
    val name = Identifier(op2.namespace, op2.name)
    override val toString = "<native: %s(%d)>".format(op2.name, 2)
  }
  
  case class LetBinding(b: Let) extends Binding with FormalBinding {
    override val toString = "@%d".format(b.nodeId)
  }  

  case class ForallDef(b: Forall) extends Binding with FormalBinding {
    override val toString = "@%d".format(b.nodeId)
  }
  
  case object NullBinding extends Binding with FormalBinding {
    override val toString = "<null>"
  }
}
