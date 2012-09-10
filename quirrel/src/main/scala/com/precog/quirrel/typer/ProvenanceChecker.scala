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
  
  override def checkProvenance(expr: Expr): Set[Error] = {
    def handleBinary(expr: Expr, left: Expr, right: Expr, relations: Map[Provenance, Set[Provenance]]): (Set[Error], Set[ProvConstraint]) = {
      val (leftErrors, leftConstr) = loop(left, relations)
      val (rightErrors, rightConstr) = loop(right, relations)
      
      val unified = unifyProvenance(relations)(left.provenance, right.provenance)
      
      val (contribErrors, contribConstr) = if (left.provenance.isParametric || right.provenance.isParametric) {
        expr.provenance = UnifiedProvenance(left.provenance, right.provenance)
        
        if (unified.isDefined)
          (Set(), Set())
        else
          (Set(), Set(Related(left.provenance, right.provenance)))
      } else {
        expr.provenance = unified getOrElse NullProvenance
        (if (unified.isDefined) Set() else Set(Error(expr, OperationOnUnrelatedSets)), Set())
      }
      
      (leftErrors ++ rightErrors ++ contribErrors, leftConstr ++ rightConstr ++ contribConstr)
    }
    
    def handleNary(expr: Expr, values: Vector[Expr], relations: Map[Provenance, Set[Provenance]]): (Set[Error], Set[ProvConstraint]) = {
      val (errorsVec, constrVec) = values map { loop(_, relations) } unzip
      
      val errors = errorsVec reduce { _ ++ _ }
      val constr = constrVec reduce { _ ++ _ }
      
      if (values.isEmpty) {
        expr.provenance = ValueProvenance
        (Set(), Set())
      } else {
        val provenances: Vector[(Provenance, Set[ProvConstraint], Boolean)] = values map { expr => (expr.provenance, Set[ProvConstraint](), true) }
        
        val (prov, constrContrib, isError) = provenances reduce { (pair1, pair2) =>
          val (prov1, constr1, error1) = pair1
          val (prov2, constr2, error2) = pair2
          
          val unified = unifyProvenance(relations)(prov1, prov2)
          
          if (prov1.isParametric || prov2.isParametric) {
            val prov = UnifiedProvenance(prov1, prov2)
            
            if (unified.isDefined)
              (unified.get, Set(), false): (Provenance, Set[ProvConstraint], Boolean)
            else
              (prov, Set(Related(prov1, prov2)) ++ constr1 ++ constr2, error1 || error2): (Provenance, Set[ProvConstraint], Boolean)
          } else {
            val prov = unified getOrElse NullProvenance
            
            (prov, constr1 ++ constr2, error1 || error2 || unified.isDefined): (Provenance, Set[ProvConstraint], Boolean)
          }
        }
        
        if (isError) {
          expr.provenance = NullProvenance
          (errors ++ Set(Error(expr, OperationOnUnrelatedSets)), constr ++ constrContrib)
        } else {
          expr.provenance = prov
          (errors, constr ++ constrContrib)
        }
      }
    }
    
    def loop(expr: Expr, relations: Map[Provenance, Set[Provenance]]): (Set[Error], Set[ProvConstraint]) = expr match {
      case expr @ Let(_, _, _, left, right) => {
        val (leftErrors, leftConst) = loop(left, relations)
        expr.constraints = leftConst
        expr.resultProvenance = left.provenance
        
        val (rightErrors, rightConst) = loop(right, relations)
        
        expr.provenance = right.provenance
        
        (leftErrors ++ rightErrors, rightConst)
      }
      
      case Solve(_, constraints, child) => {
        val (errorsVec, constrVec) = constraints map { loop(_, relations) } unzip
        
        val constrErrors = errorsVec reduce { _ ++ _ }
        val constrConstr = constrVec reduce { _ ++ _ }
        
        val (errors, constr) = loop(child, relations)
        expr.provenance = DynamicProvenance(currentId.getAndIncrement())
        
        (constrErrors ++ errors, constrConstr ++ constr)
      }
      
      case Import(_, _, child) => {
        val (errors, constr) = loop(child, relations)
        expr.provenance = child.provenance
        (errors, constr)
      }
      
      case New(_, child) => {
        val (errors, constr) = loop(child, relations)
        expr.provenance = DynamicProvenance(currentId.getAndIncrement())
        (errors, constr)
      }
      
      case Relate(_, from, to, in) => {
        val (fromErrors, fromConstr) = loop(from, relations)
        val (toErrors, toConstr) = loop(to, relations)
        
        val (contribErrors, contribConstr) = if (from.provenance.isParametric || to.provenance.isParametric) {
          (Set(), Set(NotRelated(from.provenance, to.provenance)))
        } else {
          val unified = unifyProvenance(relations)(from.provenance, to.provenance)
          
          if (unified.isDefined && unified != Some(NullProvenance))
            (Set(Error(expr, AlreadyRelatedSets)), Set())
          else
            (Set(), Set())
        }
        
        val relations2 = relations + (from.provenance -> (relations.getOrElse(from.provenance, Set()) + to.provenance))
        val relations3 = relations2 + (to.provenance -> (relations.getOrElse(to.provenance, Set()) + from.provenance))
        
        val (inErrors, inConstr) = loop(in, relations3)
        
        expr.provenance = in.provenance
        
        (fromErrors ++ toErrors ++ inErrors ++ contribErrors, fromConstr ++ toConstr ++ inConstr ++ contribConstr)
      }
      
      case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => {
        expr.provenance = ValueProvenance
        (Set(), Set())
      }
      
      case ObjectDef(_, props) => handleNary(expr, props map { _._2 }, relations)
      case ArrayDef(_, values) => handleNary(expr, values, relations)
      
      case Descent(_, child, _) => {
        val (errors, constr) = loop(child, relations)
        expr.provenance = child.provenance
        (errors, constr)
      }
      
      case Deref(_, left, right) => handleBinary(expr, left, right, relations)
      
      case expr @ Dispatch(_, name, actuals) => {
        expr.binding match {
          case LetBinding(let) => {
            val (errorsVec, constrVec) = actuals map { loop(_, relations) } unzip
            
            val actualErrors = errorsVec.fold(Set[Error]()) { _ ++ _ }
            val actualConstr = constrVec.fold(Set[ProvConstraint]()) { _ ++ _ }
            
            val ids = let.params map { Identifier(Vector(), _) }
            val zipped = ids zip (actuals map { _.provenance })
            
            def sub(target: Provenance): Provenance = {
              zipped.foldLeft(target) {
                case (target, (id, sub)) => substituteParam(id, let, target, sub)
              }
            }
            
            val (errorSet, constraintsSet) = let.constraints map {
              case Related(left, right) => {
                val optLeft2 = resolveUnifications(relations)(sub(left))
                val optRight2 = resolveUnifications(relations)(sub(right))
                
                val related = for {
                  left2 <- optLeft2
                  right2 <- optRight2
                } yield Related(left2, right2)
                
                val errors = if (related.isDefined)
                  Set[Error]()
                else
                  Set(Error(expr, OperationOnUnrelatedSets))
                
                (errors, related)
              }
              
              case NotRelated(left, right) => {
                val optLeft2 = resolveUnifications(relations)(sub(left))
                val optRight2 = resolveUnifications(relations)(sub(right))
                
                val related = for {
                  left2 <- optLeft2
                  right2 <- optRight2
                } yield NotRelated(left2, right2)
                
                val errors = if (related.isDefined)
                  Set[Error]()
                else
                  Set(Error(expr, OperationOnUnrelatedSets))
                
                (errors, related)
              }
            } unzip
            
            val resolutionErrors = errorSet.flatten
            val constraints2 = constraintsSet.flatten
            
            val mapped = constraints2 flatMap {
              case Related(left, right) if !left.isParametric && !right.isParametric => {
                if (!unifyProvenance(relations)(left, right).isDefined)
                  Some(Left(Error(expr, OperationOnUnrelatedSets)))
                else
                  None
              }
              
              case NotRelated(left, right) if !left.isParametric && !right.isParametric => {
                if (!unifyProvenance(relations)(left, right).isDefined)
                  Some(Left(Error(expr, AlreadyRelatedSets)))
                else
                  None
              }
              
              case constr => Some(Right(constr))
            }
            
            val constrErrors = mapped collect { case Left(error) => error }
            val constraints3 = mapped collect { case Right(constr) => constr }
            
            val errors = resolveUnifications(relations)(sub(let.resultProvenance)) match {
              case Some(prov) => {
                expr.provenance = prov
                Set()
              }
              
              case None => {
                expr.provenance = NullProvenance
                Set(Error(expr, OperationOnUnrelatedSets))
              }
            }
            
            val finalErrors = actualErrors ++ resolutionErrors ++ constrErrors ++ errors
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
            val (errors, constr) = loop(actuals.head, relations)
            expr.provenance = ValueProvenance
            (errors, constr)
          }
          
          case DistinctBinding => {
            val (errors, constr) = loop(actuals.head, relations)
            expr.provenance = DynamicProvenance(currentId.getAndIncrement())
            (errors, constr)
          }
          
          case LoadBinding => {
            val (errors, constr) = loop(actuals.head, relations)
            
            expr.provenance = actuals.head match {
              case StrLit(_, path) => StaticProvenance(path)
              case param if param.provenance != NullProvenance => DynamicProvenance(currentId.getAndIncrement())
              case _ => NullProvenance
            }
            
            (errors, constr)
          }
          
          case Morphism1Binding(morph1) => {
            val (errors, constr) = loop(actuals.head, relations)
            expr.provenance = if (morph1.retainIds) actuals.head.provenance else ValueProvenance
            (errors, constr)
          }
          
          case Morphism2Binding(morph2) => {
            val pair = handleBinary(expr, actuals(0), actuals(1), relations)
            
            if (!morph2.retainIds) {
              expr.provenance = ValueProvenance
            }
            
            pair
          }
          
          case Op1Binding(_) => {
            val (errors, constr) = loop(actuals.head, relations)
            expr.provenance = actuals.head.provenance
            (errors, constr)
          }
          
          case Op2Binding(_) => handleBinary(expr, actuals(0), actuals(1), relations)
          
          case NullBinding => {
            val (errorsVec, constrVec) = actuals map { loop(_, relations) } unzip
            
            val errors = errorsVec reduceOption { _ ++ _ } getOrElse Set()
            val constr = constrVec reduceOption { _ ++ _ } getOrElse Set()
            
            expr.provenance = NullProvenance
            
            (errors, constr)
          }
        }
      }
      
      case Where(_, left, right) => handleBinary(expr, left, right, relations)
      case With(_, left, right) => handleBinary(expr, left, right, relations)
      
      // TODO union/intersect/diff
      
      case Add(_, left, right) => handleBinary(expr, left, right, relations)
      case Sub(_, left, right) => handleBinary(expr, left, right, relations)
      case Mul(_, left, right) => handleBinary(expr, left, right, relations)
      case Div(_, left, right) => handleBinary(expr, left, right, relations)
      case Lt(_, left, right) => handleBinary(expr, left, right, relations)
      case LtEq(_, left, right) => handleBinary(expr, left, right, relations)
      case Gt(_, left, right) => handleBinary(expr, left, right, relations)
      case GtEq(_, left, right) => handleBinary(expr, left, right, relations)
      case Eq(_, left, right) => handleBinary(expr, left, right, relations)
      case NotEq(_, left, right) => handleBinary(expr, left, right, relations)
      case And(_, left, right) => handleBinary(expr, left, right, relations)
      case Or(_, left, right) => handleBinary(expr, left, right, relations)
      
      case Comp(_, child) => {
        val (errors, constr) = loop(child, relations)
        expr.provenance = child.provenance
        (errors, constr)
      }
      
      case Neg(_, child) => {
        val (errors, constr) = loop(child, relations)
        expr.provenance = child.provenance
        (errors, constr)
      }
      
      case Paren(_, child) => {
        val (errors, constr) = loop(child, relations)
        expr.provenance = child.provenance
        (errors, constr)
      }
    }
    
    loop(expr, Map())._1
  }
  
  private def unifyProvenance(relations: Map[Provenance, Set[Provenance]])(p1: Provenance, p2: Provenance): Option[Provenance] = (p1, p2) match {
    case (p1, p2) if p1 == p2 => Some(p1)
    
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
    
    case (ParamProvenance(id1, let1), ParamProvenance(id2, let2)) if id1 == id2 && let1 == let2 =>
      Some(ParamProvenance(id1, let1))
    
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
  
  private def substituteParam(id: Identifier, let: ast.Let, target: Provenance, sub: Provenance): Provenance = target match {
    case ParamProvenance(`id`, `let`) => sub
    
    case UnifiedProvenance(left, right) =>
      UnifiedProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))
    
    case UnionProvenance(left, right) =>
      UnionProvenance(substituteParam(id, let, left, sub), substituteParam(id, let, right, sub))
    
    case _ => target
  }
  
  private def resolveUnifications(relations: Map[Provenance, Set[Provenance]])(prov: Provenance): Option[Provenance] = prov match {
    case UnifiedProvenance(left, right) if !left.isParametric && !right.isParametric => {
      val optLeft2 = resolveUnifications(relations)(left)
      val optRight2 = resolveUnifications(relations)(right)
      
      for {
        left2 <- optLeft2
        right2 <- optRight2
        result <- unifyProvenance(relations)(left2, right2)
      } yield result
    }
    
    case UnifiedProvenance(left, right) => {
      val optLeft2 = resolveUnifications(relations)(left)
      val optRight2 = resolveUnifications(relations)(left)
      
      for {
        left2 <- optLeft2
        right2 <- optRight2
      } yield UnifiedProvenance(left2, right2)
    }
    
    case UnionProvenance(left, right) => {
      val optLeft2 = resolveUnifications(relations)(left)
      val optRight2 = resolveUnifications(relations)(left)
      
      for {
        left2 <- optLeft2
        right2 <- optRight2
      } yield left2 & right2
    }
    
    case ParamProvenance(_, _) | StaticProvenance(_) | DynamicProvenance(_) | ValueProvenance | NullProvenance =>
      Some(prov)
  }
  
  
  sealed trait Provenance {
    def &(that: Provenance) = (this, that) match {
      case (`that`, `that`) => that
      case (NullProvenance, _) => that
      case (_, NullProvenance) => this
      case _ => UnionProvenance(this, that)
    }
    
    def isParametric: Boolean
    
    def possibilities = Set(this)
  }
  
  case class ParamProvenance(id: Identifier, let: ast.Let) extends Provenance {
    override val toString = id.id
    
    val isParametric = true
  }
  
  case class UnifiedProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s >< %s)".format(left, right)
    
    val isParametric = left.isParametric || right.isParametric
  }
  
  case class UnionProvenance(left: Provenance, right: Provenance) extends Provenance {
    override val toString = "(%s & %s)".format(left, right)
    
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
  
  case object NullProvenance extends Provenance {
    override val toString = "<null>"
    val isParametric = false
  }
  
  
  sealed trait ProvConstraint
  
  case class Related(left: Provenance, right: Provenance) extends ProvConstraint
  case class NotRelated(left: Provenance, right: Provenance) extends ProvConstraint


  private case class ExprWrapper(expr: Expr) {
    override def equals(a: Any): Boolean = a match {
      case ExprWrapper(expr2) => expr equalsIgnoreLoc expr2
      case _ => false
    }

    override def hashCode = expr.hashCodeIgnoreLoc
  }
}
