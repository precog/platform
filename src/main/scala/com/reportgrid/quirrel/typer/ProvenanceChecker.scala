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
  import Utils._
  
  override def checkProvenance(expr: Expr) = {
    def loop(expr: Expr, relations: Set[(Provenance, Provenance)]): Set[Error] = expr match {
      case expr @ Let(_, _, params, left, right) => {
        val leftErrors = loop(left, relations)
        
        if (!params.isEmpty && left.provenance != NullProvenance) {
          val assumptions = expr.criticalConditions map {
            case (id, exprs) => {
              val provenances = exprs map { _.provenance }
              val unified = provenances reduce unifyProvenanceAssumingRelated
              (id -> unified)
            }
          }
          
          val unconstrained = params filterNot (assumptions contains)
          val required = unconstrained.lastOption map { params indexOf _ } map (1 +) getOrElse 0
          
          expr._assumptions() = assumptions
          expr._unconstrainedParams() = Set(unconstrained: _*)
          expr._requiredParams() = required
        } else {
          expr._assumptions() = Map()
          expr._unconstrainedParams() = Set()
          expr._requiredParams() = 0
        }
        
        val rightErrors = loop(right, relations)
        expr._provenance() = right.provenance
        
        leftErrors ++ rightErrors
      }
      
      case New(_, child) => {
        val back = loop(child, relations)
        expr._provenance() = DynamicProvenance(expr.nodeId)
        back
      }
      
      case Relate(_, from, to, in) => {
        val back = loop(from, relations) ++ loop(to, relations)
        
        val recursive = if (from.provenance == NullProvenance || to.provenance == NullProvenance) {
          expr._provenance() = NullProvenance
          Set()
        } else if (from.provenance == to.provenance || relations.contains(from.provenance -> to.provenance)) {
          expr._provenance() = NullProvenance
          Set(Error(expr, AlreadyRelatedSets))
        } else {
          val back = loop(in, relations + ((from.provenance, to.provenance)))
          expr._provenance() = in.provenance
          back
        }
        
        back ++ recursive
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
          back + Error(expr, OperationOnUnrelatedSets)
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
          back + Error(expr, OperationOnUnrelatedSets)
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
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case d @ Dispatch(_, _, exprs) => {
        val errorSets = exprs map { loop(_, relations) }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        lazy val pathParam = exprs.headOption collect {
          case StrLit(_, value) => value
        }
        
        val (prov, errors) = d.binding match {
          case BuiltIn("dataset", arity) => {
            if (exprs.length == arity)
              (pathParam map StaticProvenance getOrElse DynamicProvenance(System.identityHashCode(pathParam)), Set())
            else
              (NullProvenance, Set(Error(expr, IncorrectArity(arity, exprs.length))))
          }
          
          case BuiltIn(_, arity) => {
            if (exprs.length == arity)
              (ValueProvenance, Set())     // note: assumes all primitive functions are reductions!
            else
              (NullProvenance, Set(Error(expr, IncorrectArity(arity, exprs.length))))
          }
          
          case UserDef(e) => {
            if (exprs.length > e.params.length) {
              (NullProvenance, Set(Error(expr, IncorrectArity(e.params.length, exprs.length))))
            } else if (exprs.length < e.requiredParams) {
              val required = e.params drop exprs.length filter e.unconstrainedParams
              (NullProvenance, Set(Error(expr, UnspecifiedRequiredParams(required))))
            } else {
              val errors = for ((id, pe) <- e.params zip exprs; assumed <- e.assumptions get id) yield {
                (assumed, pe.provenance) match {
                  case (StaticProvenance(_), ValueProvenance) => None
                  case (DynamicProvenance(_), ValueProvenance) => None
                  case (ValueProvenance, _) => None
                  case (NullProvenance, _) => None
                  case (_, NullProvenance) => None
                  case _ => Some(Error(pe, SetFunctionAppliedToSet))
                }
              }
              
              val errorSet = Set(errors.flatten: _*)
              if (errorSet.isEmpty) {
                val provenances = exprs map { _.provenance }
                
                val optUnified = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
                  left flatMap { unifyProvenance(relations)(_, right) }
                }
                
                optUnified match {
                  case Some(unified) => {
                    (e.left.provenance, unified) match {
                      case (StaticProvenance(_), StaticProvenance(_)) =>
                        (NullProvenance, Set(Error(expr, SetFunctionAppliedToSet)))
                      
                      case (StaticProvenance(_), DynamicProvenance(_)) =>
                        (NullProvenance, Set(Error(expr, SetFunctionAppliedToSet)))
                      
                      case (DynamicProvenance(_), DynamicProvenance(_)) =>
                        (NullProvenance, Set(Error(expr, SetFunctionAppliedToSet)))
                      
                      case (DynamicProvenance(_), StaticProvenance(_)) =>
                        (NullProvenance, Set(Error(expr, SetFunctionAppliedToSet)))
                      
                      case (NullProvenance, _) => (NullProvenance, Set())
                      case (_, NullProvenance) => (NullProvenance, Set())
                      
                      case _ => {
                        val varAssumptions = e.assumptions ++ Map(e.params zip provenances: _*) map {
                          case (id, prov) => ((id, e), prov)
                        }
                        
                        val resultProv = computeResultProvenance(e.left, relations, varAssumptions)
                        
                        resultProv match {
                          case NullProvenance =>
                            (NullProvenance, Set(Error(expr, FunctionArgsInapplicable)))
                          
                          case _ => (resultProv, Set())
                        }
                      }
                    }
                  }
                    
                  case None => (NullProvenance, Set(Error(expr, OperationOnUnrelatedSets)))
                }
              } else {
                (NullProvenance, errorSet)
              }
            }
          }
          
          case NullBinding => (NullProvenance, Set())
        }
        
        expr._provenance() = prov
        back ++ errors
      }
      
      case Operation(_, left, _, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Add(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Sub(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Mul(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Div(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Lt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case LtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Gt(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case GtEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Eq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case NotEq(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case And(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Or(_, left, right) => {
        val back = loop(left, relations) ++ loop(right, relations)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr._provenance() = result getOrElse NullProvenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
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
  
  private def computeResultProvenance(body: Expr, relations: Set[(Provenance, Provenance)], varAssumptions: Map[(String, Let), Provenance]): Provenance = body match {
    case body @ Let(_, _, _, left, right) =>
      computeResultProvenance(right, relations, varAssumptions)
    
    case Relate(_, from, to, in) =>
      computeResultProvenance(in, relations + (from.provenance -> to.provenance), varAssumptions)
    
    case t @ TicVar(_, id) => t.binding match {
      case UserDef(lt) => varAssumptions get (id -> lt) getOrElse t.provenance
      case _ => t.provenance
    }
    
    case ObjectDef(_, props) => {
      val provenances = props map {
        case (_, e) => computeResultProvenance(e, relations, varAssumptions)
      }
      
      provenances.fold(ValueProvenance: Provenance) { (p1, p2) =>
        unifyProvenance(relations)(p1, p2) getOrElse NullProvenance
      }
    }
    
    case ArrayDef(_, values) => {
      val provenances = values map { e => computeResultProvenance(e, relations, varAssumptions) }
      
      provenances.fold(ValueProvenance: Provenance) { (p1, p2) =>
        unifyProvenance(relations)(p1, p2) getOrElse NullProvenance
      }
    }
    
    case Descent(_, child, _) => computeResultProvenance(child, relations, varAssumptions)
    
    case Deref(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case d @ Dispatch(_, _, actuals) => {
      val provenances = actuals map { e => computeResultProvenance(e, relations, varAssumptions) }
      
      if (d.isReduction) {
        d.provenance
      } else {
        d.binding match {
          case UserDef(e) => {
            val varAssumptions2 = e.assumptions ++ Map(e.params zip provenances: _*) map {
              case (id, prov) => ((id, e), prov)
            }
            
            computeResultProvenance(e.left, relations, varAssumptions ++ varAssumptions2)
          }
          
          case _ => d.provenance
        }
      }
    }
    
    case Operation(_, left, _, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Add(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Sub(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Mul(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Div(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Lt(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case LtEq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Gt(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case GtEq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Eq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case NotEq(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case And(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Or(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }
    
    case Comp(_, child) => computeResultProvenance(child, relations, varAssumptions)
    
    case Neg(_, child) => computeResultProvenance(child, relations, varAssumptions)
    
    case Paren(_, child) => computeResultProvenance(child, relations, varAssumptions)
    
    case New(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) => body.provenance
  }
  
  private def unifyProvenance(relations: Set[(Provenance, Provenance)])(p1: Provenance, p2: Provenance) = (p1, p2) match {
    case pair if relations contains pair => 
      Some(DynamicProvenance(System.identityHashCode(pair)))
    
    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      Some(StaticProvenance(path1))
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      Some(DynamicProvenance(id1))
    
    case (NullProvenance, p) => Some(NullProvenance)
    case (p, NullProvenance) => Some(NullProvenance)
    
    case (ValueProvenance, p) => Some(p)
    case (p, ValueProvenance) => Some(p)
    
    case _ => None
  }
  
  private def unifyProvenanceAssumingRelated(p1: Provenance, p2: Provenance) = (p1, p2) match {
    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      StaticProvenance(path1)
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      DynamicProvenance(id1)
    
    case (NullProvenance, p) => NullProvenance
    case (p, NullProvenance) => NullProvenance
    
    case (ValueProvenance, p) => p
    case (p, ValueProvenance) => p
    
    case pair => DynamicProvenance(System.identityHashCode(pair))
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
