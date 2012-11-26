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
package com.precog.quirrel
package typer

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import com.precog.util.IdGen
import scalaz._
import scalaz.Ordering._
import scalaz.std.option._  
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.apply._
import scalaz.syntax.semigroup._
import scalaz.syntax.order._

trait ProvenanceChecker extends parser.AST with Binder with CriticalConditionFinder {
  import Function._
  import Utils._
  import ast._
  import condition._

  private val currentId = new AtomicInteger(0)
  private val commonIds = new AtomicReference[Map[ExprWrapper, Int]](Map())
  
  override def checkProvenance(expr: Expr): Set[Error] = {
    def handleBinary(expr: Expr, left: Expr, right: Expr, relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {
      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)
      
      val unified = unifyProvenance(relations)(left.provenance, right.provenance)
      
      val (provenance, contribErrors, contribConstr) = if (left.provenance.isParametric || right.provenance.isParametric) {
        val provenance = UnifiedProvenance(left.provenance, right.provenance)
        
        if (unified.isDefined)
          (unified.get, Set(), Set())
        else
          (provenance, Set(), Set(Related(left.provenance, right.provenance)))
      } else {
        val provenance = unified getOrElse NullProvenance
        (provenance, if (unified.isDefined) Set() else Set(Error(expr, OperationOnUnrelatedSets)), Set())
      }
      
      (provenance, (leftErrors ++ rightErrors ++ contribErrors, leftConstr ++ rightConstr ++ contribConstr))
    }
    
    def handleNary(expr: Expr, values: Vector[Expr], relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {
      val (errorsVec, constrVec) = values map { loop(_, relations, constraints) } unzip
      
      val errors = errorsVec reduceOption { _ ++ _ } getOrElse Set()
      val constr = constrVec reduceOption { _ ++ _ } getOrElse Set()
      
      if (values.isEmpty) {
        (ValueProvenance, (Set(), Set()))
      } else {
        val provenances: Vector[(Provenance, Set[ProvConstraint], Boolean)] = values map { expr => (expr.provenance, Set[ProvConstraint](), false) }
        
        val (prov, constrContrib, isError) = provenances reduce { (pair1, pair2) =>
          val (prov1, constr1, error1) = pair1
          val (prov2, constr2, error2) = pair2
          
          val unified = unifyProvenance(relations)(prov1, prov2)
          
          if (prov1.isParametric || prov2.isParametric) {
            val prov = UnifiedProvenance(prov1, prov2)
            
            if (unified.isDefined)
              (unified.get, Set(), false): (Provenance, Set[ProvConstraint], Boolean)
            else
              (prov, Set(Related(prov1, prov2)) ++ constr1 ++ constr2, true): (Provenance, Set[ProvConstraint], Boolean)
          } else {
            val prov = unified getOrElse NullProvenance
            
            (prov, constr1 ++ constr2, error1 || error2 || !unified.isDefined): (Provenance, Set[ProvConstraint], Boolean)
          }
        }
        
        if (isError) {
          (NullProvenance, (errors ++ Set(Error(expr, OperationOnUnrelatedSets)), constr ++ constrContrib))
        } else {
          (prov, (errors, constr ++ constrContrib))
        }
      }
    }
    
    def handleUnion(expr: Expr, left: Expr, right: Expr, relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {
      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)
      
      val (provenance, errors, constr) = if (left.provenance == NullProvenance || right.provenance == NullProvenance) {
        val provenance = NullProvenance
        (provenance, Set(), Set())
      } else {
        val leftCard = left.provenance.cardinality
        val rightCard = right.provenance.cardinality
        
        if (left.provenance == UndefinedProvenance) {
          (right.provenance, Set(), Set())
        } else if (right.provenance == UndefinedProvenance) {
          (left.provenance, Set(), Set())
        } else if (left.provenance.isParametric || right.provenance.isParametric) {
          val card = leftCard orElse rightCard

          val provenance = card map { cardinality =>
            if (left.provenance == right.provenance)
              left.provenance
            else if (cardinality > 0)
              CoproductProvenance(left.provenance, right.provenance)
            else
              ValueProvenance
          } getOrElse DynamicDerivedProvenance(left.provenance, right.provenance)

          (provenance, Set(), Set(SameCard(left.provenance, right.provenance)))
        } else if (leftCard == rightCard) {
          val provenance =
            if (left.provenance == right.provenance)
              left.provenance
            else if (leftCard.get > 0)
              CoproductProvenance(left.provenance, right.provenance)
            else
              ValueProvenance

          (provenance, Set(), Set())
        } else {
          val provenance = NullProvenance

          (provenance, Set(Error(expr, ProductProvenanceDifferentLength)), Set())
        }
      }
      
      (provenance, (leftErrors ++ rightErrors ++ errors, leftConstr ++ rightConstr ++ constr))
    }
    
    def handleIntersect(expr: Expr, left: Expr, right: Expr, relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {
      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)

      val sameCardinality = left.provenance.cardinality == right.provenance.cardinality

      val (provenance, errors, constr) = (left.provenance, right.provenance) match {
        case (NullProvenance, _) | (_, NullProvenance) =>
          (NullProvenance, Set(), Set())

        case (UndefinedProvenance, _) | (_, UndefinedProvenance) =>
          (UndefinedProvenance, Set(), Set())

        case (a, b@DynamicProvenance(_)) if sameCardinality =>
          (CoproductProvenance(a, b), Set(), Set())

        case (a@DynamicProvenance(_), b) if sameCardinality =>
          (CoproductProvenance(a, b), Set(), Set())

        case _ =>
          val unified = unifyProvenance(Map.empty)(left.provenance, right.provenance)

          if (left.provenance.isParametric || right.provenance.isParametric) {
            val provenance =
              if (left.provenance == right.provenance)
                left.provenance
              else
                DynamicDerivedProvenance(left.provenance, right.provenance)

            (provenance, Set(), Set(SameCard(left.provenance, right.provenance)))
          } else if (unified.isDefined && sameCardinality) {
            (unified.get, Set(), Set())
          } else {
            val provenance = NullProvenance

            (provenance, Set(Error(expr, IntersectProvenanceDifferentLength)), Set())
          }
      }

      (provenance, (leftErrors ++ rightErrors ++ errors, leftConstr ++ rightConstr ++ constr))
    }

    def loop(expr: Expr, relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Set[Error], Set[ProvConstraint]) = {
      val back: (Set[Error], Set[ProvConstraint]) = expr match {
        case expr @ Let(_, _, _, left, right) => {
          val (leftErrors, leftConst) = loop(left, relations, constraints)
          expr.constraints = leftConst
          expr.resultProvenance = left.provenance
          
          val (rightErrors, rightConst) = loop(right, relations, constraints)
          
          expr.provenance = right.provenance
          
          (leftErrors ++ rightErrors, rightConst)
        }
        
        case Solve(_, solveConstr, child) => {
          val (errorsVec, constrVec) = solveConstr map { loop(_, relations, constraints) } unzip
          
          val constrErrors = errorsVec reduce { _ ++ _ }
          val constrConstr = constrVec reduce { _ ++ _ }
          
          val (errors, constr) = loop(child, relations, constraints)
          val errorSet = constrErrors ++ errors
          
          if (errorSet.nonEmpty) expr.provenance = NullProvenance
          else expr.provenance = DynamicProvenance(currentId.getAndIncrement())
          
          (errorSet, constrConstr ++ constr)
        }
        
        case Import(_, _, child) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
        
        case New(_, child) => {
          val (errors, constr) = loop(child, relations, constraints)

          if (errors.nonEmpty) expr.provenance = NullProvenance
          else expr.provenance = DynamicProvenance(currentId.getAndIncrement())

          (errors, constr)
        }
        
        case Relate(_, from, to, in) => {
          val (fromErrors, fromConstr) = loop(from, relations, constraints)
          val (toErrors, toConstr) = loop(to, relations, constraints)
          
          val unified = unifyProvenance(relations)(from.provenance, to.provenance)

          val (contribErrors, contribConstr) = if (from.provenance.isParametric || to.provenance.isParametric) {
            (Set(), Set(NotRelated(from.provenance, to.provenance)))
          } else {
            if (unified.isDefined && unified != Some(NullProvenance))
              (Set(Error(expr, AlreadyRelatedSets)), Set())
            else
              (Set(), Set())
          }
          
          val relations2 = relations + (from.provenance -> (relations.getOrElse(from.provenance, Set()) + to.provenance))
          val relations3 = relations2 + (to.provenance -> (relations.getOrElse(to.provenance, Set()) + from.provenance))
          
          val constraints2 = constraints + (from.provenance -> from) + (to.provenance -> to)
          
          val (inErrors, inConstr) = loop(in, relations3, constraints2)
          
          if (from.provenance == NullProvenance || to.provenance == NullProvenance) {
            expr.provenance = NullProvenance
          } else if (unified.isDefined || unified == Some(NullProvenance)) {
            expr.provenance = NullProvenance
          } else {
            expr.provenance = in.provenance
          }
          
          (fromErrors ++ toErrors ++ inErrors ++ contribErrors, fromConstr ++ toConstr ++ inConstr ++ contribConstr)
        }

        case UndefinedLit(_) =>
          expr.provenance = UndefinedProvenance
          (Set(), Set())

        case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => {
          expr.provenance = ValueProvenance
          (Set(), Set())
        }
        
        case ObjectDef(_, props) =>
            val (provenance, result) = handleNary(expr, props map { _._2 }, relations, constraints)
            expr.provenance = provenance
            result

        case ArrayDef(_, values) =>
            val (provenance, result) = handleNary(expr, values, relations, constraints)
            expr.provenance = provenance
            result
        
        case Descent(_, child, _) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
      
        case MetaDescent(_, child, _) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
        
        case Deref(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        
        case expr @ Dispatch(_, name, actuals) => {
          expr.binding match {
            case LetBinding(let) => {
              val (errorsVec, constrVec) = actuals map { loop(_, relations, constraints) } unzip
              
              val actualErrors = errorsVec.fold(Set[Error]()) { _ ++ _ }
              val actualConstr = constrVec.fold(Set[ProvConstraint]()) { _ ++ _ }
              
              val ids = let.params map { Identifier(Vector(), _) }
              val zipped = ids zip (actuals map { _.provenance })
              
              def sub(target: Provenance): Provenance = {
                zipped.foldLeft(target) {
                  case (target, (id, sub)) => substituteParam(id, let, target, sub)
                }
              }
              
              val constraints2 = let.constraints map {
                case Related(left, right) => {
                  val left2 = resolveUnifications(relations)(sub(left))
                  val right2 = resolveUnifications(relations)(sub(right))
                  
                  Related(left2, right2)
                }
                
                case NotRelated(left, right) => {
                  val left2 = resolveUnifications(relations)(sub(left))
                  val right2 = resolveUnifications(relations)(sub(right))
                  
                  NotRelated(left2, right2)
                }
                
                case SameCard(left, right) => {
                  val left2 = resolveUnifications(relations)(sub(left))
                  val right2 = resolveUnifications(relations)(sub(right))
                  
                  SameCard(left2, right2)
                }
              }
              
              val mapped = constraints2 flatMap {
                case Related(left, right) if !left.isParametric && !right.isParametric => {
                  if (!unifyProvenance(relations)(left, right).isDefined)
                    Some(Left(Error(expr, OperationOnUnrelatedSets)))
                  else
                    None
                }
                
                case NotRelated(left, right) if !left.isParametric && !right.isParametric => {
                  if (unifyProvenance(relations)(left, right).isDefined)
                    Some(Left(Error(expr, AlreadyRelatedSets)))
                  else
                    None
                }
                
                case SameCard(left, right) if !left.isParametric && !right.isParametric => {
                  if (left.cardinality != right.cardinality)
                    Some(Left(Error(expr, ProductProvenanceDifferentLength)))
                  else
                    None
                }
                
                case constr => Some(Right(constr))
              }
              
              val constrErrors = mapped collect { case Left(error) => error }
              val constraints3 = mapped collect { case Right(constr) => constr }
              
              expr.provenance = resolveUnifications(relations)(sub(let.resultProvenance))
              
              val finalErrors = actualErrors ++ constrErrors
              if (!finalErrors.isEmpty) {
                expr.provenance = NullProvenance
              }
              
              (finalErrors, actualConstr ++ constraints3)
            }
            
            case FormalBinding(let) => {
              expr.provenance = ParamProvenance(name, let)
              (Set(), Set())
            }
            
            case ReductionBinding(_) => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              expr.provenance = ValueProvenance
              (errors, constr)
            }
            
            case DistinctBinding => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              expr.provenance = DynamicProvenance(currentId.getAndIncrement())
              (errors, constr)
            }
            
            case LoadBinding => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              
              expr.provenance = actuals.head match {
                case StrLit(_, path) => StaticProvenance(path)
                case param if param.provenance != NullProvenance => DynamicProvenance(currentId.getAndIncrement())
                case _ => NullProvenance
              }
              
              (errors, constr)
            }
            
            case Morphism1Binding(morph1) => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              expr.provenance = if (morph1.retainIds) actuals.head.provenance else ValueProvenance
              (errors, constr)
            }
            
            case Morphism2Binding(morph2) => {
              // oddly, handleBinary doesn't seem to work here (premature fixation of expr.provenance)
              val left = actuals(0)
              val right = actuals(1)

              val (leftErrors, leftConstr) = loop(left, relations, constraints)
              val (rightErrors, rightConstr) = loop(right, relations, constraints)
                
              val unified = unifyProvenance(relations)(left.provenance, right.provenance)

              val (errors, constr) = if (morph2.retainIds) {
                if (left.provenance.isParametric || right.provenance.isParametric) {
                  expr.provenance = UnifiedProvenance(left.provenance, right.provenance)

                  if (unified.isDefined)
                    (Set(), Set())
                  else
                    (Set(), Set(Related(left.provenance, right.provenance)))
                } else {
                  expr.provenance = unified getOrElse NullProvenance
                  if (unified.isDefined)
                    (Set(), Set())
                  else
                    (Set(Error(expr, OperationOnUnrelatedSets)), Set())
                }
              } else {
                if (left.provenance.isParametric || right.provenance.isParametric) {
                  expr.provenance = ValueProvenance
                  
                  (Set(), Set(Related(left.provenance, right.provenance)))
                } else {
                  if (unified.isDefined) {
                    expr.provenance = ValueProvenance
                    (Set(), Set())
                  } else {
                    expr.provenance = NullProvenance
                    (Set(Error(expr, OperationOnUnrelatedSets)), Set())
                  }
                }
              }
              
              (leftErrors ++ rightErrors ++ errors, leftConstr ++ rightConstr ++ constr)
            }
            
            case Op1Binding(_) => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              expr.provenance = actuals.head.provenance
              (errors, constr)
            }
            
            case Op2Binding(_) =>
              val (provenance, result) = handleBinary(expr, actuals(0), actuals(1), relations, constraints)
              expr.provenance = provenance
              result
            
            case NullBinding => {
              val (errorsVec, constrVec) = actuals map { loop(_, relations, constraints) } unzip
              
              val errors = errorsVec reduceOption { _ ++ _ } getOrElse Set()
              val constr = constrVec reduceOption { _ ++ _ } getOrElse Set()
              
              expr.provenance = NullProvenance
              
              (errors, constr)
            }
          }
        }

        case Where(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result

        case Cond(_, pred, left, right) =>
          val predResult = loop(pred, relations, constraints)
          val (provenance, result) = handleUnion(expr, left, right, relations, constraints)

          val unifiedLeft = unifyProvenance(relations)(pred.provenance, left.provenance)
          val unifiedRight = unifyProvenance(relations)(pred.provenance, right.provenance)

          if (unifiedLeft.isDefined && unifiedRight.isDefined) {
            expr.provenance = provenance
            predResult |+| result
          } else {
            expr.provenance = NullProvenance
            (Set(Error(expr, OperationOnUnrelatedSets)), Set())
          }

        case With(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        
        case Union(_, left, right) =>
            val (provenance, result) = handleUnion(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result

        case Intersect(_, left, right) =>
            val (provenance, result) = handleIntersect(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        
        case Difference(_, left, right) => {
          val (leftErrors, leftConstr) = loop(left, relations, constraints)
          val (rightErrors, rightConstr) = loop(right, relations, constraints)
          
          val (errors, constr) = if (left.provenance == NullProvenance || right.provenance == NullProvenance) {
            expr.provenance = NullProvenance
            (Set(), Set())
          } else {
            val leftCard = left.provenance.cardinality
            val rightCard = right.provenance.cardinality
            
            if (left.provenance.isParametric || right.provenance.isParametric) {
              expr.provenance = left.provenance
              (Set(), Set(SameCard(left.provenance, right.provenance)))
            } else {
              if (leftCard == rightCard) {
                expr.provenance = left.provenance
                (Set(), Set())
              } else {
                expr.provenance = NullProvenance
                (Set(Error(expr, DifferenceProvenanceDifferentLength)), Set())
              }
            }
          }
          
          (leftErrors ++ rightErrors ++ errors, leftConstr ++ rightConstr ++ constr)
        }
        
        case Add(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Sub(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Mul(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Div(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Mod(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Lt(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case LtEq(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Gt(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case GtEq(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Eq(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case NotEq(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case And(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result
        case Or(_, left, right) =>
            val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
            expr.provenance = provenance
            result

        case Comp(_, child) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
        
        case Neg(_, child) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
        
        case Paren(_, child) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
      }
      
      expr.constrainingExpr = constraints get expr.provenance
      expr.relations = relations
      
      back
    }
    
    loop(expr, Map(), Map())._1
  }
  
  private def unifyProvenance(relations: Map[Provenance, Set[Provenance]])(p1: Provenance, p2: Provenance): Option[Provenance] = (p1, p2) match {
    case (p1, p2) if p1 == p2 => Some(p1)
    
    case (p1, p2) if pathExists(relations, p1, p2) || pathExists(relations, p2, p1) => 
      Some(p1 & p2)
    
    case (UndefinedProvenance, _) => Some(UndefinedProvenance)
    case (_, UndefinedProvenance) => Some(UndefinedProvenance)

    case (ProductProvenance(left, right), p2) => {
      val leftP = unifyProvenance(relations)(left, p2)
      val rightP = unifyProvenance(relations)(right, p2)
      val unionP = (leftP |@| rightP) {
        case (p1, p2) => p1 & p2
      }
      
      unionP orElse leftP orElse rightP
    }
    
    case (p1, ProductProvenance(left, right)) => {
      val leftP = unifyProvenance(relations)(p1, left)
      val rightP = unifyProvenance(relations)(p1, right)
      val unionP = (leftP |@| rightP) {
        case (p1, p2) => p1 & p2
      }

      unionP orElse leftP orElse rightP
    }
    
    case (CoproductProvenance(left, right), p2) => {
      val leftP = unifyProvenance(relations)(left, p2)
      val rightP = unifyProvenance(relations)(right, p2)
      val unionP = (leftP |@| rightP) {
        case (p1, p2) => p1 | p2
      }

      unionP orElse leftP orElse rightP
    }

    case (p1, CoproductProvenance(left, right)) => {
      val leftP = unifyProvenance(relations)(p1, left)
      val rightP = unifyProvenance(relations)(p1, right)
      val unionP = (leftP |@| rightP) {
        case (p1, p2) => p1 | p2
      }

      unionP orElse leftP orElse rightP
    }

    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      Some(StaticProvenance(path1))
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      Some(DynamicProvenance(id1))
    
    case (ParamProvenance(id1, let1), ParamProvenance(id2, let2)) if id1 == id2 && let1 == let2 =>
      Some(ParamProvenance(id1, let1))
    
    case (NullProvenance, p) => Some(NullProvenance)
    case (p, NullProvenance) => Some(NullProvenance)
    
    case (ValueProvenance, p) => Some(p)
    case (p, ValueProvenance) => Some(p)
    
    case _ => None
  }

  private def pathExists(graph: Map[Provenance, Set[Provenance]], from: Provenance, to: Provenance): Boolean = {
    // not actually DFS, but that's alright since we can't have cycles
    def dfs(seen: Set[Provenance])(from: Provenance): Boolean = {
      if (seen contains from) {
        false
      } else if (from == to) {
        true
      } else {
        val next = graph.getOrElse(from, Set())
        val seen2 = seen + from
        next exists dfs(seen2)
      }
    }
    
    dfs(Set())(from)
  }
  
  def substituteParam(id: Identifier, let: ast.Let, target: Provenance, sub: Provenance): Provenance = target match {
    case ParamProvenance(`id`, `let`) => sub
    
    case UnifiedProvenance(left, right) =>
      UnifiedProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))
    
    case ProductProvenance(left, right) =>
      ProductProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))
    
    case CoproductProvenance(left, right) =>
      CoproductProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))

    case DynamicDerivedProvenance(left, right) => {
      val left2 = substituteParam(id, let, left, sub)
      val right2 = substituteParam(id, let, right, sub)
      
      (left2.cardinality orElse right2.cardinality) map { cardinality =>
        if (left2 == right2)
          left2
        else if (cardinality > 0)
          CoproductProvenance(left2, right2)
        else
          ValueProvenance
      } getOrElse DynamicDerivedProvenance(left2, right2)
    }
    
    case _ => target
  }
  
  def resolveUnifications(relations: Map[Provenance, Set[Provenance]])(prov: Provenance): Provenance = prov match {
    case UnifiedProvenance(left, right) if !left.isParametric && !right.isParametric => {
      val left2 = resolveUnifications(relations)(left)
      val right2 = resolveUnifications(relations)(right)
      
      val optResult = unifyProvenance(relations)(left2, right2)
      optResult getOrElse (left2 & right2)
    }
    
    case UnifiedProvenance(left, right) => {
      val left2 = resolveUnifications(relations)(left)
      val right2 = resolveUnifications(relations)(right)
      
      UnifiedProvenance(left2, right2)
    }
    
    case ProductProvenance(left, right) => {
      val left2 = resolveUnifications(relations)(left)
      val right2 = resolveUnifications(relations)(right)
      left2 & right2
    }
    
    case CoproductProvenance(left, right) => {
      val left2 = resolveUnifications(relations)(left)
      val right2 = resolveUnifications(relations)(right)
      left2 | right2
    }

    case DynamicDerivedProvenance(left, right) =>
      DynamicDerivedProvenance(resolveUnifications(relations)(left), resolveUnifications(relations)(right))
    
    case ParamProvenance(_, _) | StaticProvenance(_) | DynamicProvenance(_) | ValueProvenance | UndefinedProvenance | NullProvenance =>
      prov
  }
  
  
  sealed trait Provenance {
    def &(that: Provenance) = (this, that) match {
      case (`that`, `that`) => that
      case (NullProvenance, _) => that
      case (_, NullProvenance) => this
      case _ => ProductProvenance(this, that)
    }
    
    def |(that: Provenance) = (this, that) match {
      case (`that`, `that`) => that
      case (NullProvenance, _) => NullProvenance
      case (_, NullProvenance) => NullProvenance
      case _ => CoproductProvenance(this, that)
    }

    def isParametric: Boolean
    
    def possibilities = Set(this)
    
    // TODO DynamicDerivedProvenance?
    def cardinality: Option[Int] = {
      if (isParametric || this == NullProvenance) {
        None
      } else {
        val back = possibilities filter {
          case ValueProvenance => false
          case _: ProductProvenance => false
          case _: CoproductProvenance => false
          
          // should probably remove UnifiedProvenance, but it's never going to happen
          
          case _ => true
        } size
        
        Some(back)
      }
    }


    private def associateLeft: Provenance = this match {
      case UnifiedProvenance(left, right) => 
        findChildren(this, true).toList sorted Provenance.order.toScalaOrdering reduceLeft UnifiedProvenance
      
      case ProductProvenance(left, right) =>
        findChildren(this, false).toList sorted Provenance.order.toScalaOrdering reduceLeft ProductProvenance
      
      case CoproductProvenance(left, right) =>
        findChildren(this, false).toList sorted Provenance.order.toScalaOrdering reduceLeft CoproductProvenance

      case prov => prov
    }
     
    // TODO is this too slow?
    private def findChildren(prov: Provenance, unified: Boolean): Set[Provenance] = prov match { 
      case UnifiedProvenance(left, right) if unified => findChildren(left, unified) ++ findChildren(right, unified)
      case ProductProvenance(left, right) if !unified => findChildren(left, unified) ++ findChildren(right, unified)
      case CoproductProvenance(left, right) if !unified => findChildren(left, unified) ++ findChildren(right, unified)
      case _ => Set(prov)
    }

    def makeCanonical: Provenance = {
      this match {
        case UnifiedProvenance(left, right) => UnifiedProvenance(left.makeCanonical, right.makeCanonical).associateLeft
        case ProductProvenance(left, right) => ProductProvenance(left.makeCanonical, right.makeCanonical).associateLeft
        case CoproductProvenance(left, right) => CoproductProvenance(left.makeCanonical, right.makeCanonical).associateLeft
        case DynamicDerivedProvenance(left, right) => DynamicDerivedProvenance(left.makeCanonical, right.makeCanonical)
        case prov => prov
      }
    }
  }

  object Provenance {
    implicit def order: Order[Provenance] = new Order[Provenance] {
      def order(p1: Provenance, p2: Provenance): Ordering = (p1, p2) match {
        case (ParamProvenance(id1, let1), ParamProvenance(id2, let2)) => {
          if (id1.id == id2.id) {
            if (let1 == let2) EQ
            else if (let1.loc.lineNum == let2.loc.lineNum) {
              if (let1.loc.colNum == let2.loc.colNum) EQ           // wtf??
              else if (let1.loc.colNum < let2.loc.colNum) LT
              else GT
            } else if (let1.loc.lineNum < let2.loc.lineNum) LT
            else GT
          } else if (id1.id < id2.id) LT
          else GT
        }
        case (ParamProvenance(_, _), _) => GT
        case (_, ParamProvenance(_, _)) => LT

        case (DynamicDerivedProvenance(left1, right1), DynamicDerivedProvenance(left2, right2)) => (left1 ?|? left2) |+| (right1 ?|? right2)
        case (DynamicDerivedProvenance(_, _), _) => GT
        case (_, DynamicDerivedProvenance(_, _)) => LT
    
        case (UnifiedProvenance(left1, right1), UnifiedProvenance(left2, right2)) => (left1 ?|? left2) |+| (right1 ?|? right2)
        case (UnifiedProvenance(_, _), _) => GT
        case (_, UnifiedProvenance(_, _)) => LT

        case (ProductProvenance(left1, right1), ProductProvenance(left2, right2)) => (left1 ?|? left2) |+| (right1 ?|? right2)
        case (ProductProvenance(_, _), _) => GT
        case (_, ProductProvenance(_, _)) => LT

        case (CoproductProvenance(left1, right1), CoproductProvenance(left2, right2)) => (left1 ?|? left2) |+| (right1 ?|? right2)
        case (CoproductProvenance(_, _), _) => GT
        case (_, CoproductProvenance(_, _)) => LT

        case (StaticProvenance(v1), StaticProvenance(v2)) => {
          if (v1 == v2) EQ
          else if (v1 < v2) LT
          else GT
        }
        case (StaticProvenance(_), _) => GT
        case (_, StaticProvenance(_)) => LT

        case (DynamicProvenance(v1), DynamicProvenance(v2)) => { 
          if (v1 == v2) EQ
          else if (v1 < v2) LT
          else GT
        }
        case (DynamicProvenance(_), _) => GT
        case (_, DynamicProvenance(_)) => LT

        case (ValueProvenance, _) => GT
        case (_, ValueProvenance) => LT

        case (UndefinedProvenance, UndefinedProvenance) => EQ
        case (UndefinedProvenance, _) => GT
        case (_, UndefinedProvenance) => LT

        case (NullProvenance, NullProvenance) => EQ
      }
    }
  }
  
  case class ParamProvenance(id: Identifier, let: ast.Let) extends Provenance {
    override val toString = id.id
    
    val isParametric = true
  }
  
  case class DynamicDerivedProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "@@<" + left.toString + "|" + right.toString + ">"
    
    val isParametric = left.isParametric || right.isParametric
    
    override def possibilities = left.possibilities ++ right.possibilities + this
  }
  
  case class UnifiedProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s >< %s)".format(left, right)
    
    val isParametric = left.isParametric || right.isParametric
  }
  
  case class ProductProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s & %s)".format(left, right)
    
    val isParametric = left.isParametric || right.isParametric
    
    override def possibilities = left.possibilities ++ right.possibilities + this
  }

  case class CoproductProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s | %s)".format(left, right)
    val isParametric = left.isParametric || right.isParametric

    override def possibilities = left.possibilities ++ right.possibilities + this
  }
  
  case class StaticProvenance(path: String) extends Provenance {
    override val toString = path
    val isParametric = false
  }
  
  case class DynamicProvenance(id: Int) extends Provenance {
    override val toString = "@" + id
    val isParametric = false
  }
  
  case object ValueProvenance extends Provenance {
    override val toString = "<value>"
    val isParametric = false
  }
  
  case object UndefinedProvenance extends Provenance {
    override val toString = "<undefined>"
    val isParametric = false
  }

  case object NullProvenance extends Provenance {
    override val toString = "<null>"
    val isParametric = false
  }
  
  
  sealed trait ProvConstraint
  
  case class Related(left: Provenance, right: Provenance) extends ProvConstraint
  case class NotRelated(left: Provenance, right: Provenance) extends ProvConstraint
  case class SameCard(left: Provenance, right: Provenance) extends ProvConstraint


  private case class ExprWrapper(expr: Expr) {
    override def equals(a: Any): Boolean = a match {
      case ExprWrapper(expr2) => expr equalsIgnoreLoc expr2
      case _ => false
    }

    override def hashCode = expr.hashCodeIgnoreLoc
  }
}
