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
import scalaz.std.option._  
import scalaz.syntax.apply._

trait ProvenanceChecker extends parser.AST with Binder with CriticalConditionFinder {
  import Function._
  import Utils._
  import ast._
  import condition._

  private val currentId = new AtomicInteger(0)
  private val commonIds = new AtomicReference[Map[ExprWrapper, Int]](Map())
  
  override def checkProvenance(expr: Expr) = {
    def loop(expr: Expr, relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): Set[Error] = expr match {
      case expr @ Let(_, _, params, left, right) => {
        val leftErrors = loop(left, relations, constraints)

        if (!params.isEmpty && left.provenance != NullProvenance) {
          val assumptions: Map[String, Provenance] = expr.criticalConditions map {
            case (id, exprs) => {
              val provenances = flattenForest(exprs) map { _.provenance }
              val unified = provenances reduce unifyProvenanceAssumingRelated

              (id -> unified)
            }
          }

          val accumulatedAssumptions: Map[String, Option[Vector[Provenance]]] = expr.criticalConditions map {
            case (id, exprs) => {
              val allExprs: Set[Set[Provenance]] = flattenForest(exprs) map { e => e.provenance.possibilities }

              val isMatch = allExprs.reduceOption { 
                (left, right) => left intersect right 
              } exists(p => p != ValueProvenance && p != NullProvenance)  

              val accumulatedProvenances = flattenForest(exprs) map { _.accumulatedProvenance }
              val accProv = accumulatedProvenances.reduceOption {
                (left, right) => (left <**> right) { _ ++ _ } 
              } getOrElse Some(Vector())

              val unified = {
                if (isMatch) accProv map { _.distinct }
                else accProv
              }

              (id -> unified)
            }
          }
          
          val unconstrained = params filterNot (assumptions contains)
          val required = unconstrained.lastOption map { params indexOf _ } map (1 +) getOrElse 0
          
          expr.assumptions = assumptions
          expr.accumulatedAssumptions = accumulatedAssumptions
          expr.unconstrainedParams = Set(unconstrained: _*)
          expr.requiredParams = required
        } else {
          expr.assumptions = Map()
          expr.accumulatedAssumptions = Map()
          expr.unconstrainedParams = Set()
          expr.requiredParams = 0
        }
        
        val rightErrors = loop(right, relations, constraints)
        expr.provenance = right.provenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = right.accumulatedProvenance 
        
        leftErrors ++ rightErrors
      }
      
      case Import(_, _, child) => {
        val back = loop(child, relations, constraints)
        expr.provenance = child.provenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = child.accumulatedProvenance

        back
      }
      
      case New(_, child) => {
        val back = loop(child, relations, constraints)
        expr.provenance = DynamicProvenance(currentId.incrementAndGet())
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = Some(Vector(expr.provenance))

        back
      }
      
      case expr @ Relate(_, from, to, in) => { 
        val back = loop(from, relations, constraints) ++ loop(to, relations, constraints)
        
        val recursive = if (from.provenance == NullProvenance || to.provenance == NullProvenance) {
          expr.accumulatedProvenance = None
          expr.provenance = NullProvenance
          Set()
        } else if (from.provenance == ValueProvenance || to.provenance == ValueProvenance) {
          expr.accumulatedProvenance = None
          expr.provenance = NullProvenance
          Set(Error(expr, AlreadyRelatedSets))
        } else if (pathExists(relations, from.provenance, to.provenance)) {
          expr.accumulatedProvenance = None
          expr.provenance = NullProvenance
          Set(Error(expr, AlreadyRelatedSets))
        } else {
          val fromExisting = relations.getOrElse(from.provenance, Set())
          val toExisting = relations.getOrElse(to.provenance, Set())
          
          val relations2 = relations.updated(from.provenance, fromExisting + to.provenance)
          val relations3 = relations2.updated(to.provenance, toExisting + from.provenance)
          
          val constraints2 = if (from.provenance != NullProvenance)
            constraints + (from.provenance -> from)
          else
            constraints
          
          val constraints3 = if (to.provenance != NullProvenance)
            constraints2 + (to.provenance -> to)
          else
            constraints2
          
          val back = loop(in, relations3, constraints3)
          val possibilities = in.provenance.possibilities
          
          if (possibilities.contains(from.provenance) || possibilities.contains(to.provenance)) {
            expr.accumulatedProvenance = in.accumulatedProvenance
            expr.provenance = DynamicProvenance(currentId.incrementAndGet()) 
          } else {
            expr.accumulatedProvenance = in.accumulatedProvenance
            expr.provenance = in.provenance
          }
          
          back
        }

        expr.constrainingExpr = constraints get expr.provenance
        
        back ++ recursive
      }
      
      case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => {
        expr.provenance = ValueProvenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = Some(Vector())

        Set()
      }      
      
      case ObjectDef(_, props) => { 
        val exprs: Vector[Expr] = props map { case (_, e) => e }
        val errorSets = exprs map { loop(_, relations, constraints) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        val result = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
          left flatMap { unifyProvenance(relations)(_, right) }
        }
        
        val allExprs: Vector[Set[Provenance]] = exprs map { e => e.provenance.possibilities } 

        val isMatch = allExprs.reduceOption { 
          (left, right) => left intersect right 
        } exists(p => p != ValueProvenance && p != NullProvenance)  
        
        val accProv: Option[Vector[Provenance]] = {
          exprs.map(_.accumulatedProvenance).reduceOption { 
            (left, right) => (left <**> right) { _ ++ _ } 
          } getOrElse Some(Vector())
        }

        expr.accumulatedProvenance = {
          if (isMatch) accProv map { _.distinct }
          else accProv
        }

        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case ArrayDef(_, exprs) => {  
        val errorSets = exprs map { loop(_, relations, constraints) }
        val provenances = exprs map { _.provenance }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        val result = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
          left flatMap { unifyProvenance(relations)(_, right) }
        }

        val allExprs = exprs map { e => e.provenance.possibilities }

        val isMatch = allExprs.reduceOption { 
          (left, right) => left intersect right 
        } exists(p => p != ValueProvenance && p != NullProvenance)  
        
        val accProv: Option[Vector[Provenance]] = {
          exprs.map(_.accumulatedProvenance).reduceOption { 
            (left, right) => (left <**> right) { _ ++ _ } 
          } getOrElse Some(Vector())
        }

        expr.accumulatedProvenance = {
          if (isMatch) accProv map { _.distinct }
          else accProv
        }

        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Descent(_, child, _) => {
        val back = loop(child, relations, constraints)
        expr.provenance = child.provenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = child.accumulatedProvenance

        back
      }
      
      case Deref(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance) 
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        val leftP = left.provenance.possibilities
        val rightP = right.provenance.possibilities

        val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  
        
        val accProv = for {
          leftAcc <- left.accumulatedProvenance
          rightAcc <- right.accumulatedProvenance
        } yield {
          leftAcc ++ rightAcc
        }

        expr.accumulatedProvenance = {
          if (isMatch) accProv map { _.distinct }
          else accProv
        }

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Dispatch(_, _, exprs) => {  
        val errorSets = exprs map { loop(_, relations, constraints) }
        val back = errorSets.fold(Set()) { _ ++ _ }
        
        lazy val pathParam = exprs.headOption collect {
          case StrLit(_, value) => value
        }

        def checkMappedMorphism(arity: Int) = { //TODO IS THIS CORRECT?????????????????????????????????????????????????????????????????????????????????????????
          if (exprs.length == arity) {
            expr.accumulatedProvenance = Some(Vector())
            (ValueProvenance, Set())  
          } else {
            expr.accumulatedProvenance = None
            (NullProvenance, Set(Error(expr, IncorrectArity(arity, exprs.length))))
          }
        }
        
        def checkMappedFunction(arity: Int) = {
          if (exprs.length == arity) {
            arity match {
              case 1 => {
                expr.accumulatedProvenance = exprs(0).accumulatedProvenance
                (exprs(0).provenance, Set.empty[Error])
              }

              case 2 => { 
                val result = unifyProvenance(relations)(exprs(0).provenance, exprs(1).provenance)
                val back = result getOrElse NullProvenance
                
                lazy val leftP = exprs(0).provenance.possibilities
                lazy val rightP = exprs(1).provenance.possibilities 

                lazy val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  

                lazy val accProv = for {
                  firstAcc <- exprs(0).accumulatedProvenance
                  secondAcc <- exprs(1).accumulatedProvenance
                } yield {
                  firstAcc ++ secondAcc
                }

                expr.accumulatedProvenance = {
                  if (isMatch)  accProv map { _.distinct }
                  else          accProv
                }

                val errors = {
                  if (!result.isDefined)
                    Set(Error(expr, OperationOnUnrelatedSets))
                  else
                    Set()
                }

                (back, errors)
              }

              case _ => 
                sys.error("Not implemented yet!")
            }
          } else {
            expr.accumulatedProvenance = None
            (NullProvenance, Set(Error(expr, IncorrectArity(arity, exprs.length))))
          }
        }
        
        val (prov, errors) = expr.binding match {
          case LoadBinding(_) => {
            if (exprs.length == 1) {
              lazy val prov = DynamicProvenance(provenanceId(expr))

              expr.accumulatedProvenance = Some(Vector(pathParam map StaticProvenance getOrElse prov))
              (pathParam map StaticProvenance getOrElse prov, Set())
            } else {
              expr.accumulatedProvenance = None
              (NullProvenance, Set(Error(expr, IncorrectArity(1, exprs.length))))
            }
          }

          case DistinctBinding(_) => {
            if (exprs.length == 1) {
              val prov = DynamicProvenance(currentId.incrementAndGet())
              expr.accumulatedProvenance = Some(Vector(prov))

              (prov, Set())
            } else {
              expr.accumulatedProvenance = None
              (NullProvenance, Set(Error(expr, IncorrectArity(1, exprs.length))))
            }
          }

          case Morphism1Binding(_) => checkMappedMorphism(1)

          case Morphism2Binding(_) => checkMappedMorphism(2)

          //case MorphismBinding(m) => checkMappedFunction(m.arity.toInt)

          case ReductionBinding(_) => { //assumes all reductions are arity 1
            if (exprs.length == 1) {
              expr.accumulatedProvenance = Some(Vector())
              (ValueProvenance, Set())  
            } else {
              expr.accumulatedProvenance = None
              (NullProvenance, Set(Error(expr, IncorrectArity(1, exprs.length))))
            }
          }

          case Op1Binding(_) => checkMappedFunction(1)

          case Op2Binding(_) => checkMappedFunction(2)
          
          case LetBinding(e) => {  
            if (exprs.length > e.params.length) {
              expr.accumulatedProvenance = None
              (NullProvenance, Set(Error(expr, IncorrectArity(e.params.length, exprs.length))))
            } else if (exprs.length < e.requiredParams) {
              val required = e.params drop exprs.length filter e.unconstrainedParams
              expr.accumulatedProvenance = None
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
                val accumulatedProvenances = exprs map { _.accumulatedProvenance }
                
                val optUnified = provenances.foldLeft(Some(ValueProvenance): Option[Provenance]) { (left, right) =>
                  left flatMap { unifyProvenance(relations)(_, right) }
                }
                
                optUnified match {
                  case Some(unified) => {
                    val resultPoss = e.left.provenance.possibilities
                    val resultIsSet = resultPoss exists {
                      case StaticProvenance(_) => true
                      case DynamicProvenance(_) => true
                      case _ => false
                    }
                    
                    val paramPoss = unified.possibilities
                    val paramsAreSet = paramPoss exists {
                      case StaticProvenance(_) => true
                      case DynamicProvenance(_) => true
                      case _ => false
                    }
                    
                    if (resultIsSet && paramsAreSet) {
                      expr.accumulatedProvenance = None
                      (NullProvenance, Set(Error(expr, SetFunctionAppliedToSet)))
                    } else if (resultPoss.contains(NullProvenance) || paramPoss.contains(NullProvenance)) {
                      expr.accumulatedProvenance = None
                      (NullProvenance, Set())
                    } else {
                      lazy val varAssumptions: Map[(String, Let), Provenance] = {
                        e.assumptions ++ Map(e.params zip provenances: _*) map {  
                          case (id, prov) => ((id, e), prov)
                        }
                      }

                      lazy val varAccumulatedAssumptions: Map[(String, Let), Option[Vector[Provenance]]] = {
                        e.accumulatedAssumptions ++ Map(e.params zip accumulatedProvenances: _*) map {
                          case (id, accProv) => ((id, e), accProv)
                        }
                      }
                      
                      lazy val resultProv = computeResultProvenance(e.left, relations, varAssumptions) 

                      resultProv match {
                        case NullProvenance => { //is this case reachable? 
                          expr.accumulatedProvenance = None
                          (NullProvenance, Set(Error(expr, FunctionArgsInapplicable)))
                        }
                        
                        case _ if (e.params.length == exprs.length) => {  //fully-quantified case
                          expr.accumulatedProvenance = resultProv match {
                            case ValueProvenance => Some(Vector())
                            case NullProvenance => None
                            case _ => computeResultAccumulatedProvenance(e.left, exprs, relations, varAccumulatedAssumptions) 
                          }
                          (resultProv, Set())
                        }
                        
                        case _ /* if e.params.length != exprs.length */ => {   //partially-quantified case
                          val prov = DynamicProvenance(provenanceId(expr))
                          expr.accumulatedProvenance = Some(Vector(prov))
                          (prov, Set())
                        }
                      }
                    }
                  }
                  
                  case None => {
                    expr.accumulatedProvenance = None
                    (NullProvenance, Set(Error(expr, OperationOnUnrelatedSets)))
                  }
                }
              } else {
                expr.accumulatedProvenance = None
                (NullProvenance, errorSet)
              }
            }
          }
          
          case NullBinding => {
            expr.accumulatedProvenance = None
            (NullProvenance, Set())
          }
        }
        
        expr.provenance = prov
        expr.constrainingExpr = constraints get expr.provenance

        back ++ errors
      }
      
      case Where(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        val leftP = left.provenance.possibilities
        val rightP = right.provenance.possibilities

        val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)

        val accProv = for {
          leftAcc <- left.accumulatedProvenance
          rightAcc <- right.accumulatedProvenance
        } yield {
          leftAcc ++ rightAcc
        }

        expr.accumulatedProvenance = {
          if (isMatch) accProv map { _.distinct }
          else accProv
        }

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }      

      case With(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        val leftP = left.provenance.possibilities
        val rightP = right.provenance.possibilities

        val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  

        val accProv = for {
          leftAcc <- left.accumulatedProvenance
          rightAcc <- right.accumulatedProvenance
        } yield {
          leftAcc ++ rightAcc
        }

        expr.accumulatedProvenance = {
          if (isMatch) accProv map { _.distinct }
          else accProv
        }

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      } 

      case Union(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenanceUnionIntersect(relations)(left.provenance, right.provenance)

        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance


        if (left.cardinality.isDefined && right.cardinality.isDefined) {
          if (left.cardinality == right.cardinality) {  
            expr.accumulatedProvenance = left.cardinality map { card => Vector(Stream continually DynamicProvenance(IdGen.nextInt()) take card: _*) }
            back
          } else {
            expr.accumulatedProvenance = None
            back + Error(expr, UnionProvenanceDifferentLength)
          }
        } else {
          expr.accumulatedProvenance = None
          back //assumes error generated in 'back'
        }
      }   

      case Intersect(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenanceUnionIntersect(relations)(left.provenance, right.provenance)

        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance

        if (left.cardinality.isDefined && right.cardinality.isDefined) {
          if (left.cardinality == right.cardinality) {  
            expr.accumulatedProvenance = left.cardinality map { card => Vector(Stream continually DynamicProvenance(IdGen.nextInt()) take card: _*) }
            back
          } else {
            expr.accumulatedProvenance = None
            back + Error(expr, IntersectProvenanceDifferentLength)
          }
        } else {
          expr.accumulatedProvenance = None
          back //assumes error generated in 'back'
        }
      }

      case Difference(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenanceDifference(relations)(left.provenance, right.provenance)

        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance

        if (left.cardinality.isDefined && right.cardinality.isDefined) {
          if (left.cardinality == Some(0) && right.cardinality == Some(0)) {
            expr.accumulatedProvenance = None
            back + Error(expr, DifferenceProvenanceValue)
          } else if (left.cardinality == right.cardinality) {  
            expr.accumulatedProvenance = left.accumulatedProvenance
            back
          } else {
            expr.accumulatedProvenance = None
            back + Error(expr, DifferenceProvenanceDifferentLength)
          }
        } else {
          expr.accumulatedProvenance = None
          back //assumes error generated in 'back'
        }
      }
      
      case expr @ Add(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Sub(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Mul(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Div(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Lt(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
                
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ LtEq(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Gt(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
               
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ GtEq(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Eq(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ NotEq(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)

        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ And(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case expr @ Or(_, left, right) => {
        val back = loop(left, relations, constraints) ++ loop(right, relations, constraints)
        val result = unifyProvenance(relations)(left.provenance, right.provenance)
        expr.provenance = result getOrElse NullProvenance
        expr.constrainingExpr = constraints get expr.provenance
        
        expr.accumulatedProvenance = determineBinaryAccProv(left, right)
        
        if (!result.isDefined)
          back + Error(expr, OperationOnUnrelatedSets)
        else
          back
      }
      
      case Comp(_, child) => {
        val back = loop(child, relations, constraints)
        expr.provenance = child.provenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = child.accumulatedProvenance

        back
      }
      
      case Neg(_, child) => {
        val back = loop(child, relations, constraints)
        expr.provenance = child.provenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = child.accumulatedProvenance

        back
      }
      
      case Paren(_, child) => {
        val back = loop(child, relations, constraints)
        expr.provenance = child.provenance
        expr.constrainingExpr = constraints get expr.provenance

        expr.accumulatedProvenance = child.accumulatedProvenance

        back
      }
    }
    
    loop(expr, Map(), Map())
  }

  private def computeResultProvenance(body: Expr, relations: Map[Provenance, Set[Provenance]], varAssumptions: Map[(String, Let), Provenance]): Provenance = body match {
    case body @ Let(_, _, _, left, right) =>
      computeResultProvenance(right, relations, varAssumptions)

    case Import(_, _, child) =>
      computeResultProvenance(child, relations, varAssumptions)
    
    case Relate(_, from, to, in) => {
      val fromExisting = relations.getOrElse(from.provenance, Set())
      val toExisting = relations.getOrElse(to.provenance, Set())
      
      val relations2 = relations.updated(from.provenance, fromExisting + to.provenance)
      val relations3 = relations2.updated(to.provenance, toExisting + from.provenance)
      
      val possibilities = in.provenance.possibilities
      
      if (possibilities.contains(from.provenance) || possibilities.contains(to.provenance))
        DynamicProvenance(provenanceId(body))
      else
        computeResultProvenance(in, relations3, varAssumptions)
    }
    
    case t @ TicVar(_, id) => t.binding match {
      case LetBinding(lt) => varAssumptions get (id -> lt) getOrElse t.provenance
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
      val provenances = actuals map { ex => computeResultProvenance(ex, relations, varAssumptions) }
      
      if (d.isReduction) {
        d.provenance
      } else {
        d.binding match {
          case LetBinding(e) if (e.params.length == actuals.length) => { 
            val varAssumptions2 = e.assumptions ++ Map(e.params zip provenances: _*) map {
              case (id, prov) => ((id, e), prov)
            }
            computeResultProvenance(e.left, relations, varAssumptions ++ varAssumptions2)
          }
          case Op1Binding(f) => {
            computeResultProvenance(actuals(0), relations, varAssumptions)
          }
          case Op2Binding(f) => {
            val left = computeResultProvenance(actuals(0), relations, varAssumptions)
            val right = computeResultProvenance(actuals(1), relations, varAssumptions)

            unifyProvenance(relations)(left, right) getOrElse NullProvenance
          }
          case _ => d.provenance  
        }
      }
    }
    
    case Where(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }    
   
    case With(_, left, right) => {
      val leftProv = computeResultProvenance(left, relations, varAssumptions)
      val rightProv = computeResultProvenance(right, relations, varAssumptions)
      unifyProvenance(relations)(leftProv, rightProv) getOrElse NullProvenance
    }    
   
    case Union(_, _, _) | Intersect(_, _, _) | Difference(_, _, _) => body.provenance

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
    
    case New(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => body.provenance   
  }

  private def computeResultAccumulatedProvenance(body: Expr, exprs: Vector[Expr], relations: Map[Provenance, Set[Provenance]], varAccumulatedAssumptions: Map[(String, Let), Option[Vector[Provenance]]]): Option[Vector[Provenance]] = body match {
    case Let(_, _, params, left, right) => {
      computeResultAccumulatedProvenance(right, exprs, relations, varAccumulatedAssumptions)
    }

    case Import(_, _, child) => 
      computeResultAccumulatedProvenance(child, exprs, relations, varAccumulatedAssumptions)

    case n @ New(_, child) => {
      val prov = n.provenance
      Some(Vector(prov))
    }

    case Relate(_, from, to, in) => {
      if (from.provenance == NullProvenance || to.provenance == NullProvenance) None
      else if (from.provenance == ValueProvenance || to.provenance == ValueProvenance) None
      else if (pathExists(relations, from.provenance, to.provenance)) None
      else computeResultAccumulatedProvenance(in, exprs, relations, varAccumulatedAssumptions)
    }

    case StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => Some(Vector())

    case t @ TicVar(_, id) => t.binding match {
      case LetBinding(lt) => varAccumulatedAssumptions get (id -> lt) getOrElse Some(Vector())
      case _ => Some(Vector())
    }

    case ObjectDef(_, props) => {
      val exprs: Vector[Expr] = props map { case (_, e) => e }
      val allExprs: Vector[Set[Provenance]] = exprs map { e => e.provenance.possibilities }

      val isMatch = allExprs.reduceOption { 
        (left, right) => left intersect right 
      } exists(p => p != ValueProvenance && p != NullProvenance)  
      
      val accProv: Option[Vector[Provenance]] = {
        exprs.map(_.accumulatedProvenance).reduceOption { 
          (left, right) => (left <**> right) { _ ++ _ } 
        } getOrElse Some(Vector())
      }

      if (isMatch) accProv map { _.distinct }
      else accProv
    }

    case ArrayDef(_, exprs2) => {
      val allExprs = exprs2 map { e => e.provenance.possibilities }

      val isMatch = allExprs.reduceOption { 
        (left, right) => left intersect right 
      } exists(p => p != ValueProvenance && p != NullProvenance)  
      
      val accProv: Option[Vector[Provenance]] = {
        exprs2.map(_.accumulatedProvenance).reduceOption { 
          (left, right) => (left <**> right) { _ ++ _ } 
        } getOrElse Some(Vector())
      }

      if (isMatch) accProv map { _.distinct }
      else accProv
    }

    case Descent(_, child, _) => computeResultAccumulatedProvenance(child, exprs, relations, varAccumulatedAssumptions)

    case Deref(_, left, right) => {
      val leftP = left.provenance.possibilities
      val rightP = right.provenance.possibilities

      val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  
      
      val accProv = for {
        leftAcc <- computeResultAccumulatedProvenance(left, exprs, relations, varAccumulatedAssumptions)
        rightAcc <- computeResultAccumulatedProvenance(right, exprs, relations, varAccumulatedAssumptions)
      } yield {
        leftAcc ++ rightAcc
      }

      if (isMatch) accProv map { _.distinct }
      else accProv
    }

    case d @ Dispatch(_, _, actuals) => {
      lazy val accProvenances = actuals map { e => computeResultAccumulatedProvenance(e, exprs, relations, varAccumulatedAssumptions) }

      if (d.isReduction) {
        Some(Vector())
      } else {
        d.binding match {
          case LetBinding(e) if e.params.length == actuals.length => {
            val varAccumulatedAssumptions2: Map[(String, Let), Option[Vector[Provenance]]] = {
              e.accumulatedAssumptions ++ Map(e.params zip accProvenances: _*) map {
                case (id, accProv) => ((id, e), accProv)
              }
            }
            computeResultAccumulatedProvenance(e.left, exprs, relations, varAccumulatedAssumptions ++ varAccumulatedAssumptions2)
          }
          case LetBinding(e) if e.params.length < actuals.length => {
            val prov = DynamicProvenance(provenanceId(d))
            Some(Vector(prov))
          }
          case _ => d.accumulatedProvenance
        }
      }
    }

    case Where(_, left, right) => {
      val leftP = left.provenance.possibilities
      val rightP = right.provenance.possibilities

      val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  
      
      val accProv = for {
        leftAcc <- computeResultAccumulatedProvenance(left, exprs, relations, varAccumulatedAssumptions)
        rightAcc <- computeResultAccumulatedProvenance(right, exprs, relations, varAccumulatedAssumptions)
      } yield {
        leftAcc ++ rightAcc
      }

      if (isMatch) accProv map { _.distinct }
      else accProv
    }

    case With(_, left, right) => {
      val leftP = left.provenance.possibilities
      val rightP = right.provenance.possibilities

      val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  
      
      val accProv = for {
        leftAcc <- computeResultAccumulatedProvenance(left, exprs, relations, varAccumulatedAssumptions)
        rightAcc <- computeResultAccumulatedProvenance(right, exprs, relations, varAccumulatedAssumptions)
      } yield {
        leftAcc ++ rightAcc
      }

      if (isMatch) accProv map { _.distinct }
      else accProv
    }

    case Union(_, left, right) => {
      if (left.cardinality.isDefined && right.cardinality.isDefined) {
        if (left.cardinality == right.cardinality) {  
          left.cardinality map { card => Vector(Stream continually DynamicProvenance(IdGen.nextInt()) take card: _*) }
        } else {
          None
        }
      } else {
        None
      }
    }

    case Intersect(_, left, right) => {
      if (left.cardinality.isDefined && right.cardinality.isDefined) {
        if (left.cardinality == right.cardinality) {  
          left.cardinality map { card => Vector(Stream continually DynamicProvenance(IdGen.nextInt()) take card: _*) }
        } else {
          None
        }
      } else {
        None
      }
    }

    case Difference(_, left, right) => {
      if (left.cardinality.isDefined && right.cardinality.isDefined) {
        if (left.cardinality == right.cardinality) {  
          if (left.cardinality == Some(0)) None
          else computeResultAccumulatedProvenance(left, exprs, relations, varAccumulatedAssumptions)
        } else {
          None
        }
      } else {
        None
      }
    }

    case Add(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Sub(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Mul(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Div(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Lt(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case LtEq(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Gt(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case GtEq(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Eq(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case NotEq(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case And(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Or(_, left, right) => {
      determineResultBinaryAccProv(left, right, exprs, relations, varAccumulatedAssumptions)
    }

    case Comp(_, child) => {
      computeResultAccumulatedProvenance(child, exprs, relations, varAccumulatedAssumptions)
    }

    case Neg(_, child) => {
      computeResultAccumulatedProvenance(child, exprs, relations, varAccumulatedAssumptions)
    }

    case Paren(_, child) => {
      computeResultAccumulatedProvenance(child, exprs, relations, varAccumulatedAssumptions)
    }
  }

  private def determineBinaryAccProv(left: Expr, right: Expr): Option[Vector[Provenance]] = {
    val leftP = left.provenance.possibilities
    val rightP = right.provenance.possibilities

    val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  
    
    val accProv = for {
      leftAcc <- left.accumulatedProvenance
      rightAcc <- right.accumulatedProvenance
    } yield {
      leftAcc ++ rightAcc
    }
    
    if (isMatch) accProv map { _.distinct }
    else accProv
  }

  private def determineResultBinaryAccProv(left: Expr, right: Expr, exprs: Vector[Expr], relations: Map[Provenance, Set[Provenance]], varAccumulatedAssumptions: Map[(String, Let), Option[Vector[Provenance]]]): Option[Vector[Provenance]] = {
    val leftP = left.provenance.possibilities
    val rightP = right.provenance.possibilities

    val isMatch = leftP.intersect(rightP).exists(p => p != ValueProvenance && p != NullProvenance)  
    
    val accProv = for {
      leftAcc <- computeResultAccumulatedProvenance(left, exprs, relations, varAccumulatedAssumptions)
      rightAcc <- computeResultAccumulatedProvenance(right, exprs, relations, varAccumulatedAssumptions)
    } yield {
      leftAcc ++ rightAcc
    }
    
    if (isMatch) accProv map { _.distinct }
    else accProv
  }

  private def unifyProvenance(relations: Map[Provenance, Set[Provenance]])(p1: Provenance, p2: Provenance): Option[Provenance] = (p1, p2) match {
    case (p1, p2) if pathExists(relations, p1, p2) || pathExists(relations, p2, p1) => 
      Some(p1 & p2)
    
    case (UnionProvenance(left, right), p2) => {
      val leftP = unifyProvenance(relations)(left, p2)
      val rightP = unifyProvenance(relations)(right, p2)
      val unionP = (leftP.toList zip rightP.toList headOption) map {
        case (p1, p2) => p1 & p2
      }
      
      unionP orElse leftP orElse rightP
    }
    
    case (p1, UnionProvenance(left, right)) => {
      val leftP = unifyProvenance(relations)(p1, left)
      val rightP = unifyProvenance(relations)(p1, right)
      val unionP = (leftP.toList zip rightP.toList headOption) map {
        case (p1, p2) => p1 & p2
      }
      
      unionP orElse leftP orElse rightP
    }
    
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

  private def unifyProvenanceUnionIntersect(relations: Map[Provenance, Set[Provenance]])(p1: Provenance, p2: Provenance): Option[Provenance] = (p1, p2) match {
    case (NullProvenance, p) => Some(NullProvenance)
    case (p, NullProvenance) => Some(NullProvenance)

    case (p1, p2) => Some(DynamicProvenance(currentId.incrementAndGet()))
  }

  private def unifyProvenanceDifference(relations: Map[Provenance, Set[Provenance]])(p1: Provenance, p2: Provenance): Option[Provenance] = (p1, p2) match {
    case (NullProvenance, p) => Some(NullProvenance)
    case (p, NullProvenance) => Some(NullProvenance)

    case (p1, p2) => Some(p1)
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
  
  // needed in the emitter
  private[quirrel] def unifyProvenanceAssumingRelated(p1: Provenance, p2: Provenance) = (p1, p2) match {
    case (StaticProvenance(path1), StaticProvenance(path2)) if path1 == path2 => 
      StaticProvenance(path1)
    
    case (DynamicProvenance(id1), DynamicProvenance(id2)) if id1 == id2 =>
      DynamicProvenance(id1)
    
    case (NullProvenance, p) => NullProvenance
    case (p, NullProvenance) => NullProvenance
    
    case (ValueProvenance, p) => p
    case (p, ValueProvenance) => p
    
    case pair => DynamicProvenance(currentId.incrementAndGet())
  }
  
  private def flattenForest(forest: Set[ConditionTree]): Set[Expr] = forest flatMap {
    case Condition(expr) => Set(expr)
    case Reduction(_, forest) => flattenForest(forest)
  }

  private def provenanceId(expr: Expr): Int = {
    val wrapper = ExprWrapper(expr)

    def loop(id: Int): Int = {
      val orig = commonIds.get()
      if (orig contains wrapper)
        orig(wrapper)
      else if (commonIds.compareAndSet(orig, orig + (wrapper -> id)))
        id
      else
        loop(id)
    }
    
    loop(currentId.incrementAndGet())
  }
  
  sealed trait Provenance {
    def &(that: Provenance) = (this, that) match {
      case (`that`, `that`) => that
      case (NullProvenance, _) => that
      case (_, NullProvenance) => this
      case _ => UnionProvenance(this, that)
    }
    
    def possibilities = Set(this)
  }
  
  case class UnionProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s & %s)".format(left, right)
    
    override def possibilities = left.possibilities ++ right.possibilities + this
  }
  
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


  private case class ExprWrapper(expr: Expr) {
    override def equals(a: Any): Boolean = a match {
      case ExprWrapper(expr2) => expr equalsIgnoreLoc expr2
      case _ => false
    }

    override def hashCode = expr.hashCodeIgnoreLoc
  }
}
