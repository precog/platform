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
package com.reportgrid.quirrel
package typer

trait ProvenanceChecker extends parser.AST with Binder {
  
  override def checkProvenance(expr: Expr) = {
    val Message = "cannot perform operation on unrelated sets"
    
    def loop(expr: Expr, relations: Set[(Provenance, Provenance)]): Set[Error] = expr match {
      case Let(_, _, _, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        expr._provenance() = right.provenance
        back
      }
      
      case New(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = DynamicProvenance(expr.nodeId)
        back
      }
      
      case Relate(_, from, to, in) => {
        val back = loop(from, relations) ++
          loop(to, relations) ++
          loop(in, relations + ((from.provenance, to.provenance)))
          
        expr._provenance() = in.provenance
        back
      }
      
      case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) => {
        expr._provenance() = ValueProvenance
        Set()
      }
      
      case ObjectDef(_, props) => {
        val exprs = props map { case (_, e) => e }
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        val result = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
          left flatMap { unifyProvenance(relations)(_, right) }
        }
        
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case ArrayDef(_, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        val result = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
          left flatMap { unifyProvenance(relations)(_, right) }
        }
        
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Descent(_, child, _) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }
      
      case Deref(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case d @ Dispatch(_, _, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        val paramProvenance = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
          left flatMap { unifyProvenance(relations)(_, right) }
        }
        
        lazy val pathParam = exprs.headOption collect {
          case StrLit(_, value) => value
        }
        
        expr._provenance() = d.binding match {
          case BuiltIn("dataset") =>
            pathParam map StaticProvenance getOrElse DynamicProvenance(System.identityHashCode(pathParam))
          
          case BuiltIn(_) => ValueProvenance     // note: assumes all primitive functions are reductions!
          case UserDef(e) => e.left.provenance
          case NullBinding => NullProvenance
        }
        
        if (!paramProvenance.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Operation(_, left, _, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Add(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Sub(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Mul(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Div(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Lt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case LtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Gt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case GtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Eq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case NotEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case And(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Or(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, Message)
        else
          back
      }
      
      case Comp(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }
      
      case Neg(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }
      
      case Paren(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = child.provenance
        back
      }                                    
    }                                                                           
                                                                                      
    loop(expr, Set())
  }                                                                 
  
  def unifyProvenance(relations: Set[(Provenance, Provenance)])(p1: Provenance, p2: Provenance) = (p1, p2) match {
    case pair if relations contains pair => 
      Some(DynamicProvenance(System.identityHashCode(pair)))
    
    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      Some(StaticProvenance(path1))
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      Some(DynamicProvenance(id1))
    
    case (NullProvenance, p) => Some(p)
    case (p, NullProvenance) => Some(p)
    
    case (ValueProvenance, p) => Some(p)
    case (p, ValueProvenance) => Some(p)
    
    case _ => None
  }
  
  sealed trait Provenance
  
  case class StaticProvenance(path: String) extends Provenance {
    override val toString = path
  }
  
  case class DynamicProvenance(id: Int) extends Provenance {
    override val toString = "@" + id
  }
  
  case object ValueProvenance extends Provenance {
    override val toString = "<value>"
  }
  
  case object NullProvenance extends Provenance {
    override val toString = "<null>"
  }
}
