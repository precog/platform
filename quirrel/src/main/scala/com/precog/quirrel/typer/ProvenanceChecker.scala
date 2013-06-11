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

import com.precog.bytecode.IdentityPolicy
import com.precog.util.IdGen

import scalaz._
import scalaz.Ordering._
import scalaz.std.option._  
import scalaz.std.set._
import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.syntax.apply._
import scalaz.syntax.semigroup._
import scalaz.syntax.order._
import scalaz.syntax.monoid._

trait ProvenanceChecker extends parser.AST with Binder {
  import library._
  import Function._
  import Utils._
  import ast._

  private val currentId = new AtomicInteger(0)
  private val commonIds = new AtomicReference[Map[ExprWrapper, Int]](Map())
  
  override def checkProvenance(expr: Expr): Set[Error] = {
    def handleBinary(
        expr: Expr,
        left: Expr,
        right: Expr,
        relations: Map[Provenance, Set[Provenance]],
        constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {

      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)
      
      val unified = unifyProvenance(relations)(left.provenance, right.provenance)
      
      val (provenance, contribErrors, contribConstr) = {
        if ((left.provenance == InfiniteProvenance || right.provenance == InfiniteProvenance) && expr.disallowsInfinite) {
          val provenance = NullProvenance
          val errors = Set(Error(expr, CannotUseDistributionWithoutSampling))
          
          (provenance, errors, Set())
        } else if (left.provenance.isParametric || right.provenance.isParametric) {
          if (unified.isDefined) {
            (unified.get, Set(), Set())
          } else {
            val provenance = UnifiedProvenance(left.provenance, right.provenance)
            (provenance, Set(), Set(Related(left.provenance, right.provenance)))
          }
        } else {
          val provenance = unified getOrElse NullProvenance
          (provenance, if (unified.isDefined) Set() else Set(Error(expr, OperationOnUnrelatedSets)), Set())
        }
      }
      
      (provenance, (leftErrors ++ rightErrors ++ contribErrors, leftConstr ++ rightConstr ++ contribConstr))
    }
    
    def handleNary(
        expr: Expr,
        values: Vector[Expr],
        relations: Map[Provenance, Set[Provenance]],
        constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {

      val (errorsVec, constrVec) = values map { loop(_, relations, constraints) } unzip
      
      val errors = errorsVec reduceOption { _ ++ _ } getOrElse Set()
      val constr = constrVec reduceOption { _ ++ _ } getOrElse Set()
      
      if (values.isEmpty) {
        (ValueProvenance, (Set(), Set()))
      } else {
        val provenances: Vector[(Provenance, Set[ProvConstraint], Set[Error])] =
          values map { expr => (expr.provenance, Set[ProvConstraint](), Set[Error]()) }
        
        val (prov, constrContrib, errorsContrib) = provenances reduce { (pair1, pair2) =>
          val (prov1, constr1, error1) = pair1
          val (prov2, constr2, error2) = pair2

          val addedErrors = error1 ++ error2
          val addedConstr = constr1 ++ constr2
          
          val unified = unifyProvenance(relations)(prov1, prov2)
          
          if ((prov1 == InfiniteProvenance || prov2 == InfiniteProvenance) && expr.disallowsInfinite) {
            val errors = Set(Error(expr, CannotUseDistributionWithoutSampling))
            (NullProvenance, addedConstr, addedErrors ++ errors)
          } else if (prov1.isParametric || prov2.isParametric) {
            if (unified.isDefined) {
              (unified.get, addedConstr, addedErrors): (Provenance, Set[ProvConstraint], Set[Error])
            } else {
              val prov = UnifiedProvenance(prov1, prov2)
              (prov, addedConstr ++ Set(Related(prov1, prov2)), addedErrors): (Provenance, Set[ProvConstraint], Set[Error])
            }
          } else if (unified.isDefined) {
            (unified.get, addedConstr, addedErrors): (Provenance, Set[ProvConstraint], Set[Error])
          } else {
            val errors = Set(Error(expr, OperationOnUnrelatedSets))
            (NullProvenance, addedConstr, addedErrors ++ errors): (Provenance, Set[ProvConstraint], Set[Error])
          }
        }
        
        (prov, (errors ++ errorsContrib, constr ++ constrContrib))
      }
    }

    def handleUnionLike(tpe: ErrorType)(left: Provenance, right: Provenance) = {
      lazy val leftCard = left.cardinality
      lazy val rightCard = right.cardinality

      if (left == right)
        (left, Set(), Set())

      else if (left == NullProvenance || right == NullProvenance)
        (NullProvenance, Set(), Set())

      else if (left == InfiniteProvenance || right == InfiniteProvenance)
        (NullProvenance, Set(Error(expr, CannotUseDistributionWithoutSampling)), Set())

      else if (left == UndefinedProvenance)
        (right, Set(), Set())

      else if (right == UndefinedProvenance)
        (left, Set(), Set())

      else if (left.isParametric || right.isParametric)
        (DerivedUnionProvenance(left, right), Set(), Set(SameCard(left, right, tpe)))
      
      else if (leftCard == rightCard)
        (CoproductProvenance(left, right), Set(), Set())

      else
        (NullProvenance, Set(Error(expr, tpe)), Set())
    }

    // Similar to handleUnion but must ensure that pred's provenance
    // can be unified with both sides.
    def handleCond(
        expr: Expr,
        pred: Expr,
        left: Expr,
        right: Expr,
        relations: Map[Provenance, Set[Provenance]],
        constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {

      val (predErrors, predConstr) = loop(pred, relations, constraints)
      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)

      val (provenance, errors, constr) = {
        if (pred.provenance == NullProvenance || left.provenance == NullProvenance || right.provenance == NullProvenance) {
          (NullProvenance, Set(), Set())
        } else if (pred.provenance == InfiniteProvenance || left.provenance == InfiniteProvenance || right.provenance == InfiniteProvenance) {
          (NullProvenance, Set(Error(expr, CannotUseDistributionWithoutSampling)), Set())
        } else {
          val leftUnified = unifyProvenance(relations)(pred.provenance, left.provenance)
          val rightUnified = unifyProvenance(relations)(pred.provenance, right.provenance)

          (leftUnified |@| rightUnified) (handleUnionLike(CondProvenanceDifferentLength)) getOrElse {
            (NullProvenance, Set(Error(expr, OperationOnUnrelatedSets)), Set())
          }
        }
      }
        
      val finalErrors = predErrors ++ leftErrors ++ rightErrors ++ errors
      val finalConstr = predConstr ++ leftConstr ++ rightConstr ++ constr

      (provenance, (finalErrors, finalConstr))
    }

    def handleUnion(
        expr: Expr, 
        left: Expr,
        right: Expr, 
        relations: Map[Provenance, Set[Provenance]],
        constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {

      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)
      
      val (provenance, errors, constr) = if (left.provenance == InfiniteProvenance || right.provenance == InfiniteProvenance) {
        val errors = Set(Error(expr, CannotUseDistributionWithoutSampling))
        (NullProvenance, errors, Set())
      } else {
        handleUnionLike(UnionProvenanceDifferentLength)(left.provenance, right.provenance)
      }
      
      val finalErrors = leftErrors ++ rightErrors ++ errors
      val finalConstrs = leftConstr ++ rightConstr ++ constr

      (provenance, (finalErrors, finalConstrs))
    }

    // different from unifyProvenace in that:
    // unifyProvenance(foo&bar, bar&baz) = Some((foo&bar)&baz)
    // hasCommonality(foo&bar, bar&baz) = false
    // this way we can statically reject this query:
    // | //foo ~ //bar ~ //baz
    // | (//foo + //bar) intersect (//bar + //baz)
    def hasCommonality(left: Provenance, right: Provenance): Boolean = (left, right) match {
      case (CoproductProvenance(prov1, prov2), pr) =>
        hasCommonality(prov1, pr) || hasCommonality(prov2, pr) 

      case (pl, CoproductProvenance(prov1, prov2)) =>
        hasCommonality(pl, prov1) || hasCommonality(pl, prov2)

      case (ProductProvenance(l1, l2), ProductProvenance(r1, r2)) => {
        val leftOk = hasCommonality(l1, r1) || hasCommonality(l1, r2)
        val rightOk = hasCommonality(l2, r1) || hasCommonality(l2, r2)
        leftOk && rightOk
      }

      case (pl, pr) => pl == pr 
    }

    // A DynamicProvenance being involved in either side means
    // we can't _prove_ the data is coming from different sets,
    // since `(new //foo) intersect (new //foo)` returns `new //foo`
    def dynamicPossibility(prov: Provenance) = prov.possibilities exists {
      case DynamicProvenance(_) => true
      case _ => false
    }

    def hasDynamic(left: Provenance, right: Provenance) =
      dynamicPossibility(left) && dynamicPossibility(right)

    def handleIntersect(
        expr: Expr,
        left: Expr,
        right: Expr,
        relations: Map[Provenance, Set[Provenance]],
        constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {

      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)

      lazy val isCommon = hasCommonality(left.provenance, right.provenance)
      lazy val isDynamic = hasDynamic(left.provenance, right.provenance)

      lazy val leftCard = left.provenance.cardinality
      lazy val rightCard = right.provenance.cardinality

      val (prov, errors, constr) = {
        if (left.provenance == right.provenance)
          (left.provenance, Set(), Set())

        else if (left.provenance == NullProvenance || right.provenance == NullProvenance)
          (NullProvenance, Set(), Set())

        else if (left.provenance == UndefinedProvenance && right.provenance == UndefinedProvenance)
          (UndefinedProvenance, Set(), Set())

        else if (left.provenance == UndefinedProvenance || right.provenance == UndefinedProvenance)
          (NullProvenance, Set(Error(expr, IntersectWithNoCommonalities)), Set())

        else if (left.provenance == InfiniteProvenance || right.provenance == InfiniteProvenance)
          (NullProvenance, Set(Error(expr, CannotUseDistributionWithoutSampling)), Set())

        else if (left.provenance.isParametric || right.provenance.isParametric) {
          val provenance = DerivedIntersectProvenance(left.provenance, right.provenance)

          val sameCard = SameCard(left.provenance, right.provenance, IntersectProvenanceDifferentLength)
          val commonality = Commonality(left.provenance, right.provenance, IntersectWithNoCommonalities)

          (provenance, Set(), Set(sameCard, commonality))
        }
        
        else if (!(leftCard == rightCard))
          (NullProvenance, Set(Error(expr, IntersectProvenanceDifferentLength)), Set())

        else if (isCommon) {
          val unified = unifyProvenance(relations)(left.provenance, right.provenance)
          val prov = unified getOrElse CoproductProvenance(left.provenance, right.provenance)
          (prov, Set(), Set())
        }
        
        else if (isDynamic)
          (CoproductProvenance(left.provenance, right.provenance), Set(), Set())

        // Can prove that result set is empty. Operation disallowed.
        else
          (NullProvenance, Set(Error(expr, IntersectWithNoCommonalities)), Set())
      }

      val finalErrors = leftErrors ++ rightErrors ++ errors
      val finalConstr = leftConstr ++ rightConstr ++ constr
      
      (prov, (finalErrors, finalConstr))
    }

    def handleDifference(
        expr: Expr,
        left: Expr,
        right: Expr,
        relations: Map[Provenance, Set[Provenance]],
        constraints: Map[Provenance, Expr]): (Provenance, (Set[Error], Set[ProvConstraint])) = {

      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)

      lazy val isCommon = hasCommonality(left.provenance, right.provenance)
      lazy val isDynamic = hasDynamic(left.provenance, right.provenance)

      lazy val leftCard = left.provenance.cardinality
      lazy val rightCard = right.provenance.cardinality

      val (prov, errors, constr) = {
        if (left.provenance == right.provenance)
          (left.provenance, Set(), Set())
        
        else if (left.provenance == NullProvenance || right.provenance == NullProvenance)
          (NullProvenance, Set(), Set())

        else if (left.provenance == UndefinedProvenance && right.provenance == UndefinedProvenance)
          (UndefinedProvenance, Set(), Set())

        else if (left.provenance == UndefinedProvenance || right.provenance == UndefinedProvenance)
          (NullProvenance, Set(Error(expr, DifferenceWithNoCommonalities)), Set())

        else if (left.provenance == InfiniteProvenance || right.provenance == InfiniteProvenance)
          (NullProvenance, Set(Error(expr, CannotUseDistributionWithoutSampling)), Set())

        else if (left.provenance.isParametric || right.provenance.isParametric) {
          val provenance = DerivedDifferenceProvenance(left.provenance, right.provenance)

          val sameCard = SameCard(left.provenance, right.provenance, DifferenceProvenanceDifferentLength)
          val commonality = Commonality(left.provenance, right.provenance, DifferenceWithNoCommonalities)

          (provenance, Set(), Set(sameCard, commonality))
        }

        else if (!(leftCard == rightCard))
          (NullProvenance, Set(Error(expr, DifferenceProvenanceDifferentLength)), Set())

        // (foo|bar difference foo) provenance may have foo|bar provenance
        else if (isCommon || isDynamic) 
          (left.provenance, Set(), Set())

        // Can prove that result set is the entire LHS. Operation disallowed.
        else
          (NullProvenance, Set(Error(expr, DifferenceWithNoCommonalities)), Set())
      }

      val finalErrors = leftErrors ++ rightErrors ++ errors
      val finalConstr = leftConstr ++ rightConstr ++ constr
      
      (prov, (finalErrors, finalConstr))
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
        
        case Assert(_, pred, child) => {
          val (predErrors, predConst) = loop(pred, relations, constraints)
          val (childErrors, childConst) = loop(child, relations, constraints)

          val assertErrors = {
            if (pred.provenance == InfiniteProvenance)
              Set(Error(expr, CannotUseDistributionWithoutSampling))
            else 
              Set()
          }

          if (pred.provenance != NullProvenance && pred.provenance != InfiniteProvenance)
            expr.provenance = child.provenance
          else
            expr.provenance = NullProvenance
          
          (predErrors ++ childErrors ++ assertErrors, predConst ++ childConst)
        }
        
        case Observe(_, data, samples) => {
          val (dataErrors, dataConst) = loop(data, relations, constraints)
          val (samplesErrors, samplesConst) = loop(samples, relations, constraints)

          val observeDataErrors = if (data.provenance == InfiniteProvenance) {
            Set(Error(expr, CannotUseDistributionWithoutSampling))
          } else {
            Set()
          }
          
          val observeSamplesErrors = if (samples.provenance != InfiniteProvenance) {
            Set(Error(expr, CannotUseDistributionWithoutSampling))
          } else {
            Set()
          }
          
          if (data.provenance != InfiniteProvenance && samples.provenance == InfiniteProvenance)
            expr.provenance = data.provenance
          else
            expr.provenance = NullProvenance

          (dataErrors ++ samplesErrors ++ observeDataErrors ++ observeSamplesErrors, dataConst ++ samplesConst)
        }
        
        case New(_, child) => {
          val (errors, constr) = loop(child, relations, constraints)

          if (errors.nonEmpty)
            expr.provenance = NullProvenance
          else if (child.provenance == InfiniteProvenance)
            expr.provenance = InfiniteProvenance
          else if (child.provenance.isParametric)
            // We include an identity in ParametricDynamicProvenance so that we can
            // distinguish two `New` nodes that have the same `child`, each assigning
            // different identities.
            // | f(x) :=
            // |   y := new x
            // |   z := new x
            // |   y + z
            // | f(5)
            expr.provenance = ParametricDynamicProvenance(child.provenance, currentId.getAndIncrement())
          else
            expr.provenance = DynamicProvenance(currentId.getAndIncrement())

          (errors, constr)
        }
        
        case Relate(_, from, to, in) => {
          val (fromErrors, fromConstr) = loop(from, relations, constraints)
          val (toErrors, toConstr) = loop(to, relations, constraints)
          
          val unified = unifyProvenance(relations)(from.provenance, to.provenance)

          val (contribErrors, contribConstr) = if (from.provenance == InfiniteProvenance || to.provenance == InfiniteProvenance) {
            (Set(Error(expr, CannotUseDistributionWithoutSampling)), Set())
          } else if (from.provenance.isParametric || to.provenance.isParametric) {
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
          
          if (from.provenance == NullProvenance || to.provenance == NullProvenance || from.provenance == InfiniteProvenance || to.provenance == InfiniteProvenance) {
            expr.provenance = NullProvenance
          } else if (unified.isDefined || unified == Some(NullProvenance)) {
            expr.provenance = NullProvenance
          } else if (in.provenance == InfiniteProvenance) {
            expr.provenance = InfiniteProvenance
          } else {
            expr.provenance = in.provenance
          }

          val finalErrors = fromErrors ++ toErrors ++ inErrors ++ contribErrors
          val finalConstrs = fromConstr ++ toConstr ++ inConstr ++ contribConstr
          
          (finalErrors, finalConstrs)
        }

        case UndefinedLit(_) => {
          expr.provenance = UndefinedProvenance
          (Set(), Set())
        }

        case TicVar(_, _) | Literal(_) => {
          expr.provenance = ValueProvenance
          (Set(), Set())
        }

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
                
                case SameCard(left, right, tpe) => {
                  val left2 = resolveUnifications(relations)(sub(left))
                  val right2 = resolveUnifications(relations)(sub(right))
                  
                  SameCard(left2, right2, tpe)
                }

                case Commonality(left, right, tpe) => {
                  val left2 = resolveUnifications(relations)(sub(left))
                  val right2 = resolveUnifications(relations)(sub(right))

                  Commonality(left2, right2, tpe)
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
                
                case SameCard(left, right, tpe) if !left.isParametric && !right.isParametric => {
                  if (left.cardinality != right.cardinality)
                    Some(Left(Error(expr, tpe)))
                  else
                    None
                }

                case Commonality(left, right, tpe) if !left.isParametric && !right.isParametric => {
                  if (!hasCommonality(left, right))
                    Some(Left(Error(expr, tpe)))
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

              val reductionErrors = if (actuals.head.provenance == InfiniteProvenance) {
                Set(Error(expr, CannotUseDistributionWithoutSampling))
              } else {
                Set()
              }

              if (actuals.head.provenance == InfiniteProvenance) {
                expr.provenance = NullProvenance
              } else {
                expr.provenance = ValueProvenance
              }

              (errors ++ reductionErrors, constr)
            }
            
            case DistinctBinding => {
              val (errors, constr) = loop(actuals.head, relations, constraints)

              val distinctErrors = if (actuals.head.provenance == InfiniteProvenance) {
                Set(Error(expr, CannotUseDistributionWithoutSampling))
              } else {
                Set()
              }

              if (actuals.head.provenance == InfiniteProvenance) {
                expr.provenance = NullProvenance
              } else {
                expr.provenance = DynamicProvenance(currentId.getAndIncrement())
              }

              (errors ++ distinctErrors, constr)
            }
            
            case LoadBinding | RelLoadBinding => {      // FIXME not the same as StaticProvenance!
              val (errors, constr) = loop(actuals.head, relations, constraints)
              
              expr.provenance = actuals.head match {
                case StrLit(_, path) => StaticProvenance(path)
                
                case d @ Dispatch(_, _, Vector(StrLit(_, path))) if d.binding == ExpandGlobBinding =>
                  StaticProvenance(path)
                
                case param if param.provenance != NullProvenance => DynamicProvenance(currentId.getAndIncrement())
                case _ => NullProvenance
              }
              
              (errors, constr)
            }
            
            case ExpandGlobBinding => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              expr.provenance = actuals.head.provenance
              (errors, constr)
            }
            
            /* There are some subtle issues here regarding consistency between the compiler's
             * notion of `cardinality` and the evaluators's notion of `identities`. If a morphism
             * (of 1 or 2 parameters) has IdentityPolicy.Product(_, _), then we correctly unify
             * the provenances of the LHS and RHS, and the notions of identities will be equivalent.
             * But if a morphism has IdentityPolicy.Cross, were the LHS and RHS share some provenances,
             * then we have no notion of provenance that will capture this information. Just for the 
             * sake of understanding the problem, we *could* have `CrossProvenance(_, _)`, but this
             * type would only serve to alert us to the fact that there are definitely identities
             * coming from both sides. And this is not the correct thing to do. Once we have record-
             * based identities, we will no longer need to do cardinality checking in the compiler,
             * and this problem will go away.
             */
            case Morphism1Binding(morph1) => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              expr.provenance = {
                if (morph1.isInfinite) {
                  InfiniteProvenance
                } else {
                  def rec(policy: IdentityPolicy): Provenance = policy match {
                    case IdentityPolicy.Product(left, right) => {
                      val recLeft = rec(left)
                      val recRight = rec(right)
                      unifyProvenance(relations)(recLeft, recRight) getOrElse ProductProvenance(recLeft, recRight)
                    }

                    case (_: IdentityPolicy.Retain) => actuals.head.provenance
                    
                    case IdentityPolicy.Synthesize => {
                      if (actuals.head.provenance.isParametric)
                        ParametricDynamicProvenance(actuals.head.provenance, currentId.getAndIncrement())
                      else
                        DynamicProvenance(currentId.getAndIncrement())
                    }
                    
                    case IdentityPolicy.Strip => ValueProvenance
                  }
                  rec(morph1.idPolicy)
                }
              }
              (errors, constr)
            }
            
            case Morphism2Binding(morph2) => {
              // oddly, handleBinary doesn't seem to work here (premature fixation of expr.provenance)
              val left = actuals(0)
              val right = actuals(1)

              val (leftErrors, leftConstr) = loop(left, relations, constraints)
              val (rightErrors, rightConstr) = loop(right, relations, constraints)

              val unified = unifyProvenance(relations)(left.provenance, right.provenance)

              def compute(paramProv: Provenance, prov: Provenance): (Set[Error], Set[ProvConstraint], Provenance) = {
                if (left.provenance.isParametric || right.provenance.isParametric) {
                  if (unified.isDefined)
                    (Set(), Set(), paramProv)
                  else
                    (Set(), Set(Related(left.provenance, right.provenance)), paramProv)
                } else {
                  if (unified.isDefined)
                    (Set(), Set(), prov)
                  else
                    (Set(Error(expr, OperationOnUnrelatedSets)), Set(), NullProvenance)
                }
              }

              def rec(policy: IdentityPolicy): (Set[Error], Set[ProvConstraint], Provenance) = policy match {
                case IdentityPolicy.Product(left0, right0) =>
                  val (leftErrors, leftConst, leftProv) = rec(left0)
                  val (rightErrors, rightConst, rightProv) = rec(right0)

                  val prov = unifyProvenance(relations)(leftProv, rightProv) getOrElse ProductProvenance(leftProv, rightProv)
                  val (err, const, finalProv) = compute(prov, prov)

                  val errors = leftErrors ++ rightErrors ++ err
                  val consts = leftConst ++ rightConst ++ const

                  (errors, consts, finalProv)

                /* TODO The `Cross` case is not currently correct! 
                 * When we call `cardinality` on a morphism with this IdentityPolicy
                 * if left.provenance and right.provenance contain equivalent provenances,
                 * incorrect cardinality will be returned. For example, if we have Cross
                 * where LHS=//foo and RHS=//foo, the `cardinality` will be size 1,
                 * when it should be size 2. (See above comment. This bug will be able to be
                 * smoothly resolved with the addition of record-based ids.)
                 */
                case IdentityPolicy.Retain.Cross => {
                  val product = ProductProvenance(left.provenance, right.provenance)
                  val prov = unifyProvenance(relations)(left.provenance, right.provenance) getOrElse product

                  compute(prov, prov)
                }
                
                case IdentityPolicy.Retain.Left =>
                  compute(left.provenance, left.provenance)
                  
                case IdentityPolicy.Retain.Right =>
                  compute(right.provenance, right.provenance)
                
                case IdentityPolicy.Retain.Merge => {
                  val paramProv = UnifiedProvenance(left.provenance, right.provenance)
                  val prov = unified getOrElse NullProvenance
                  compute(paramProv, prov)
                }
                
                case IdentityPolicy.Synthesize => {
                  val paramProv = ParametricDynamicProvenance(UnifiedProvenance(left.provenance, right.provenance), currentId.getAndIncrement())
                  val prov = DynamicProvenance(currentId.getAndIncrement())
                  compute(paramProv, prov)
                }
                
                case IdentityPolicy.Strip =>
                  compute(ValueProvenance, ValueProvenance)
              }

              val (errors, constr, prov) = rec(morph2.idPolicy)

              expr.provenance = prov
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

        case Cond(_, pred, left, right) => {
          val (provenance, result) = handleCond(expr, pred, left, right, relations, constraints)
          expr.provenance = provenance
          result
        }

        case Union(_, left, right) => {
          val (provenance, result) = handleUnion(expr, left, right, relations, constraints)
          expr.provenance = provenance
          result
        }

        case Intersect(_, left, right) => {
          val (provenance, result) = handleIntersect(expr, left, right, relations, constraints)
          expr.provenance = provenance
          result
        }
        
        case Difference(_, left, right) => {
          val (provenance, result) = handleDifference(expr, left, right, relations, constraints)
          expr.provenance = provenance
          result
        }

        case UnaryOp(_, child) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
        
        case BinaryOp(_, left, right) => {
          val (provenance, result) = handleBinary(expr, left, right, relations, constraints)
          expr.provenance = provenance
          result
        }
        
        // TODO change to NaryOp(_, values) once scalac bugs are resolved
        case expr: NaryOp => {
          val (provenance, result) = handleNary(expr, expr.values, relations, constraints)
          expr.provenance = provenance
          result
        }
      }

      expr.constrainingExpr = constraints get expr.provenance
      expr.relations = relations
      
      back
    }
    
    val (errors, constraints) = loop(expr, Map(), Map())

    val lastErrors = {
      if (expr.provenance == InfiniteProvenance) Set(Error(expr, CannotUseDistributionWithoutSampling))
      else Set()
    }
    
    errors ++ lastErrors
  }

  def unifyProvenance(relations: Map[Provenance, Set[Provenance]])(p1: Provenance, p2: Provenance): Option[Provenance] = (p1, p2) match {
    case (p1, p2) if p1 == p2 => Some(p1)
    
    case (p1, p2) if pathExists(relations, p1, p2) || pathExists(relations, p2, p1) => 
      Some(p1 & p2)

    case (NullProvenance, p) => Some(NullProvenance)
    case (p, NullProvenance) => Some(NullProvenance)
    
    case (UndefinedProvenance, _) => Some(UndefinedProvenance)
    case (_, UndefinedProvenance) => Some(UndefinedProvenance)

    case (ValueProvenance, p) => Some(p)
    case (p, ValueProvenance) => Some(p)

    case (ProductProvenance(left, right), p2) => {
      val leftP = unifyProvenance(relations)(left, p2)
      val rightP = unifyProvenance(relations)(right, p2)

      val unionP = (leftP |@| rightP) {
        case (p1, p2) => p1 & p2
      }

      lazy val unionLeft = leftP map { ProductProvenance(_, right) }
      lazy val unionRight = rightP map { ProductProvenance(left, _) }
      
      unionP orElse unionLeft orElse unionRight
    }
    
    case (p1, ProductProvenance(left, right)) => {
      val leftP = unifyProvenance(relations)(p1, left)
      val rightP = unifyProvenance(relations)(p1, right)

      val unionP = (leftP |@| rightP) {
        case (p1, p2) => p1 & p2
      }

      lazy val unionLeft = leftP map { ProductProvenance(_, right) }
      lazy val unionRight = rightP map { ProductProvenance(left, _) }

      unionP orElse unionLeft orElse unionRight
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

  def components(prov: Provenance, target: Provenance): Boolean = {
    def recursePair(left: Provenance, right: Provenance) =
      components(left, target) || components(right, target)

    prov match {
      case DerivedUnionProvenance(left, right) => recursePair(left, right)
      case DerivedIntersectProvenance(left, right) => recursePair(left, right)
      case DerivedDifferenceProvenance(left, right) => recursePair(left, right)
      case UnifiedProvenance(left, right) => recursePair(left, right)
      case ProductProvenance(left, right) => recursePair(left, right)
      case CoproductProvenance(left, right) => recursePair(left, right)
      case ParametricDynamicProvenance(p, _) => components(p, target)
      case _ => prov == target
    }
  }
  
  // errors are propagated through provenance constraints
  // hence are not needed here
  def substituteParam(id: Identifier, let: ast.Let, target: Provenance, sub: Provenance): Provenance = target match {
    case ParamProvenance(`id`, `let`) => sub

    case ParametricDynamicProvenance(prov, _) if components(prov, ParamProvenance(`id`, `let`)) =>
      DynamicProvenance(currentId.getAndIncrement())

    case UnifiedProvenance(left, right) =>
      UnifiedProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))
    
    case ProductProvenance(left, right) =>
      ProductProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))
    
    case CoproductProvenance(left, right) =>
      CoproductProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))

    case DerivedUnionProvenance(left, right) => {
      val left2 = substituteParam(id, let, left, sub)
      val right2 = substituteParam(id, let, right, sub)

      if (left2 == right2)
        left2
      else if (!(left2.isParametric && right2.isParametric)) 
        CoproductProvenance(left2, right2)
      else
        DerivedUnionProvenance(left2, right2)
    }

    case DerivedIntersectProvenance(left, right) => {
      val left2 = substituteParam(id, let, left, sub)
      val right2 = substituteParam(id, let, right, sub)

      if (left2 == right2) {
        left2
      } else if (!(left2.isParametric && right2.isParametric)) {
        val unified = unifyProvenance(Map())(left2, right2)
        unified getOrElse CoproductProvenance(left2, right2)
      } else {
        DerivedIntersectProvenance(left2, right2)
      }
    }

    case DerivedDifferenceProvenance(left, right) => {
      val left2 = substituteParam(id, let, left, sub)
      val right2 = substituteParam(id, let, right, sub)

      if (left2 == right2 || (!(left2.isParametric && right2.isParametric)))
        left2
      else
        DerivedDifferenceProvenance(left2, right2)
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

    case DerivedUnionProvenance(left, right) => {
      val left2 = resolveUnifications(relations)(left)
      val right2 = resolveUnifications(relations)(right)
      DerivedUnionProvenance(left2, right2)
    }
    
    case DerivedIntersectProvenance(left, right) => {
      val left2 = resolveUnifications(relations)(left)
      val right2 = resolveUnifications(relations)(right)
      DerivedIntersectProvenance(left2, right2)
    }
    
    case DerivedDifferenceProvenance(left, right) => {
      val left2 = resolveUnifications(relations)(left)
      val right2 = resolveUnifications(relations)(right)
      DerivedDifferenceProvenance(left2, right2)
    }
    
    case ParamProvenance(_, _) |
        ParametricDynamicProvenance(_, _) |
        StaticProvenance(_) |
        DynamicProvenance(_) |
        ValueProvenance |
        UndefinedProvenance |
        NullProvenance |
        InfiniteProvenance => prov
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

    def cardinality: Option[Int]
    
    private def associateLeft: Provenance = this match {
      case UnifiedProvenance(_, _) => 
        findChildren(this, true).toList sorted Provenance.order.toScalaOrdering reduceLeft UnifiedProvenance
      
      case ProductProvenance(_, _) =>
        findChildren(this, false).toList sorted Provenance.order.toScalaOrdering reduceLeft ProductProvenance
      
      case CoproductProvenance(_, _) =>
        findChildren(this, false).toList sorted Provenance.order.toScalaOrdering reduceLeft CoproductProvenance

      case DerivedUnionProvenance(_, _) =>
        findChildren(this, false).toList sorted Provenance.order.toScalaOrdering reduceLeft DerivedUnionProvenance

      case DerivedIntersectProvenance(_, _) =>
        findChildren(this, false).toList sorted Provenance.order.toScalaOrdering reduceLeft DerivedIntersectProvenance

      case DerivedDifferenceProvenance(_, _) =>
        findChildren(this, false).toList sorted Provenance.order.toScalaOrdering reduceLeft DerivedDifferenceProvenance

      case prov => prov
    }
     
    // TODO is this too slow?
    private def findChildren(prov: Provenance, unified: Boolean): Set[Provenance] = prov match { 
      case UnifiedProvenance(left, right) if unified => findChildren(left, unified) ++ findChildren(right, unified)
      case ProductProvenance(left, right) if !unified => findChildren(left, unified) ++ findChildren(right, unified)
      case CoproductProvenance(left, right) if !unified => findChildren(left, unified) ++ findChildren(right, unified)
      case DerivedUnionProvenance(left, right) if !unified => findChildren(left, unified) ++ findChildren(right, unified)
      case DerivedIntersectProvenance(left, right) if !unified => findChildren(left, unified) ++ findChildren(right, unified)
      case DerivedDifferenceProvenance(left, right) if !unified => findChildren(left, unified) ++ findChildren(right, unified)
      case _ => Set(prov)
    }

    def makeCanonical: Provenance = {
      this match {
        case UnifiedProvenance(left, right) => UnifiedProvenance(left.makeCanonical, right.makeCanonical).associateLeft
        case ProductProvenance(left, right) => ProductProvenance(left.makeCanonical, right.makeCanonical).associateLeft
        case CoproductProvenance(left, right) => CoproductProvenance(left.makeCanonical, right.makeCanonical).associateLeft
        case DerivedUnionProvenance(left, right) => DerivedUnionProvenance(left.makeCanonical, right.makeCanonical)
        case DerivedIntersectProvenance(left, right) => DerivedIntersectProvenance(left.makeCanonical, right.makeCanonical)
        case DerivedDifferenceProvenance(left, right) => DerivedDifferenceProvenance(left.makeCanonical, right.makeCanonical)
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

        case (ParametricDynamicProvenance(prov1, id1), ParametricDynamicProvenance(prov2, id2)) => {
          if (prov1 == prov2) {
            if (id1 == id2) EQ
            else if (id1 < id2) LT
            else GT
          } else {
            prov1 ?|? prov2
          }
        }
        case (ParametricDynamicProvenance(_, _), _) => GT
        case (_, ParametricDynamicProvenance(_, _)) => LT

        case (DerivedUnionProvenance(left1, right1), DerivedUnionProvenance(left2, right2)) => (left1 ?|? left2) |+| (right1 ?|? right2)
        case (DerivedUnionProvenance(_, _), _) => GT
        case (_, DerivedUnionProvenance(_, _)) => LT
    
        case (DerivedIntersectProvenance(left1, right1), DerivedIntersectProvenance(left2, right2)) => (left1 ?|? left2) |+| (right1 ?|? right2)
        case (DerivedIntersectProvenance(_, _), _) => GT
        case (_, DerivedIntersectProvenance(_, _)) => LT
    
        case (DerivedDifferenceProvenance(left1, right1), DerivedDifferenceProvenance(left2, right2)) => (left1 ?|? left2) |+| (right1 ?|? right2)
        case (DerivedDifferenceProvenance(_, _), _) => GT
        case (_, DerivedDifferenceProvenance(_, _)) => LT
    
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

        case (InfiniteProvenance, InfiniteProvenance) => EQ
        case (InfiniteProvenance, _) => GT
        case (_, InfiniteProvenance) => LT

        case (NullProvenance, NullProvenance) => EQ
      }
    }
  }
  
  case class ParamProvenance(id: Identifier, let: ast.Let) extends Provenance {
    override val toString = "$" + id.id
    val isParametric = true
    def cardinality: Option[Int] = None
  }
  
  case class DerivedUnionProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "@@U<" + left.toString + "|" + right.toString + ">"

    val isParametric = left.isParametric || right.isParametric

    override def possibilities = left.possibilities ++ right.possibilities + this

    def cardinality: Option[Int] = None
  }
  
  case class DerivedIntersectProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "@@I<" + left.toString + "|" + right.toString + ">"

    val isParametric = left.isParametric || right.isParametric

    override def possibilities = left.possibilities ++ right.possibilities + this

    def cardinality: Option[Int] = None
  }
  
  case class DerivedDifferenceProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "@@D<" + left.toString + "|" + right.toString + ">"

    val isParametric = left.isParametric || right.isParametric

    override def possibilities = left.possibilities ++ right.possibilities + this

    def cardinality: Option[Int] = None
  }
  
  case class UnifiedProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s >< %s)".format(left, right)

    val isParametric = left.isParametric || right.isParametric

    def cardinality: Option[Int] = left.cardinality |+| right.cardinality
  }
  
  case class ProductProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s & %s)".format(left, right)

    val isParametric = left.isParametric || right.isParametric

    override def possibilities = left.possibilities ++ right.possibilities + this

    def cardinality = left.cardinality |+| right.cardinality
  }

  case class CoproductProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s | %s)".format(left, right)

    val isParametric = left.isParametric || right.isParametric

    override def possibilities = left.possibilities ++ right.possibilities + this

    def cardinality: Option[Int] = if (left.cardinality == right.cardinality) {
      left.cardinality
    } else {
      sys.error("cardinality invariant not upheld")
    }
  }
  
  case class StaticProvenance(path: String) extends Provenance {
    override val toString = path
    val isParametric = false
    def cardinality: Option[Int] = Some(1)
  }
  
  case class DynamicProvenance(id: Int) extends Provenance {
    override val toString = "@" + id
    val isParametric = false
    def cardinality: Option[Int] = Some(1)
  }
  
  case class ParametricDynamicProvenance(prov: Provenance, id: Int) extends Provenance {
    override val toString = "@@" + prov.toString
    val isParametric = true
    def cardinality: Option[Int] = Some(1)
  }
  
  case object ValueProvenance extends Provenance {
    override val toString = "<value>"
    val isParametric = false
    def cardinality: Option[Int] = Some(0)
  }
  
  case object InfiniteProvenance extends Provenance {
    override val toString = "<infinite>"
    val isParametric = false
    def cardinality: Option[Int] = None  //todo remove me ahhhhhh
  }
  
  case object UndefinedProvenance extends Provenance {
    override val toString = "<undefined>"
    val isParametric = false
    def cardinality: Option[Int] = None
  }

  case object NullProvenance extends Provenance {
    override val toString = "<null>"
    val isParametric = false
    def cardinality: Option[Int] = None
  }
  
  
  sealed trait ProvConstraint
  
  case class Related(left: Provenance, right: Provenance) extends ProvConstraint
  case class NotRelated(left: Provenance, right: Provenance) extends ProvConstraint
  case class SameCard(left: Provenance, right: Provenance, tpe: ErrorType) extends ProvConstraint
  case class Commonality(left: Provenance, right: Provenance, tpe: ErrorType) extends ProvConstraint
}
