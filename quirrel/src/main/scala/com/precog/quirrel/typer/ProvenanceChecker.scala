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
    def handleBinary(expr: Expr, left: Expr, right: Expr, relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Set[Error], Set[ProvConstraint]) = {
      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)
      
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
    
    def handleNary(expr: Expr, values: Vector[Expr], relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Set[Error], Set[ProvConstraint]) = {
      val (errorsVec, constrVec) = values map { loop(_, relations, constraints) } unzip
      
      val errors = errorsVec reduceOption { _ ++ _ } getOrElse Set()
      val constr = constrVec reduceOption { _ ++ _ } getOrElse Set()
      
      if (values.isEmpty) {
        expr.provenance = ValueProvenance
        (Set(), Set())
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
          expr.provenance = NullProvenance
          (errors ++ Set(Error(expr, OperationOnUnrelatedSets)), constr ++ constrContrib)
        } else {
          expr.provenance = prov
          (errors, constr ++ constrContrib)
        }
      }
    }
    
    def handleIUI(expr: Expr, left: Expr, right: Expr, relations: Map[Provenance, Set[Provenance]], constraints: Map[Provenance, Expr]): (Set[Error], Set[ProvConstraint]) = {
      val (leftErrors, leftConstr) = loop(left, relations, constraints)
      val (rightErrors, rightConstr) = loop(right, relations, constraints)
      
      val (errors, constr) = if (left.provenance == NullProvenance || right.provenance == NullProvenance) {
        expr.provenance = NullProvenance
        (Set(), Set())
      } else {
        val leftCard = left.provenance.cardinality
        val rightCard = right.provenance.cardinality
        
        if (left.provenance.isParametric || right.provenance.isParametric) {
          val card = leftCard orElse rightCard
          
          expr.provenance = card map { cardinality =>
            Stream continually { DynamicProvenance(currentId.getAndIncrement()): Provenance } take cardinality reduceOption UnionProvenance getOrElse ValueProvenance
          } getOrElse DynamicDerivedProvenance(left.provenance)
          
          (Set(), Set(SameCard(left.provenance, right.provenance)))
        } else {
          if (leftCard == rightCard) {
            expr.provenance = Stream continually { DynamicProvenance(currentId.getAndIncrement()): Provenance } take leftCard.get reduceOption UnionProvenance getOrElse ValueProvenance
            (Set(), Set())
          } else {
            expr.provenance = NullProvenance
            
            val errorType = expr match {
              case _: Union => UnionProvenanceDifferentLength
              case _: Intersect => IntersectProvenanceDifferentLength
              case _ => sys.error("unreachable")
            }
            
            (Set(Error(expr, errorType)), Set())
          }
        }
      }
      
      (leftErrors ++ rightErrors ++ errors, leftConstr ++ rightConstr ++ constr)
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
          expr.provenance = DynamicProvenance(currentId.getAndIncrement())
          
          (constrErrors ++ errors, constrConstr ++ constr)
        }
        
        case Import(_, _, child) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
        
        case New(_, child) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = DynamicProvenance(currentId.getAndIncrement())
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
        
        case TicVar(_, _) | StrLit(_, _) | NumLit(_, _) | BoolLit(_, _) | NullLit(_) => {
          expr.provenance = ValueProvenance
          (Set(), Set())
        }
        
        case ObjectDef(_, props) => handleNary(expr, props map { _._2 }, relations, constraints)
        case ArrayDef(_, values) => handleNary(expr, values, relations, constraints)
        
        case Descent(_, child, _) => {
          val (errors, constr) = loop(child, relations, constraints)
          expr.provenance = child.provenance
          (errors, constr)
        }
        
        case Deref(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        
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
                
                case SameCard(left, right) => {
                  val optLeft2 = resolveUnifications(relations)(sub(left))
                  val optRight2 = resolveUnifications(relations)(sub(right))
                  
                  val same = for {
                    left2 <- optLeft2
                    right2 <- optRight2
                  } yield SameCard(left2, right2)
                  
                  val errors = if (same.isDefined)
                    Set[Error]()
                  else
                    Set(Error(expr, OperationOnUnrelatedSets))
                  
                  (errors, same)
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
                
                case SameCard(left, right) if !left.isParametric && !right.isParametric => {
                  if (left.cardinality != right.cardinality)
                    Some(Left(Error(expr, UnionProvenanceDifferentLength)))
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
              val pair = handleBinary(expr, actuals(0), actuals(1), relations, constraints)
              
              if (!morph2.retainIds) {
                expr.provenance = ValueProvenance
              }
              
              pair
            }
            
            case Op1Binding(_) => {
              val (errors, constr) = loop(actuals.head, relations, constraints)
              expr.provenance = actuals.head.provenance
              (errors, constr)
            }
            
            case Op2Binding(_) => handleBinary(expr, actuals(0), actuals(1), relations, constraints)
            
            case NullBinding => {
              val (errorsVec, constrVec) = actuals map { loop(_, relations, constraints) } unzip
              
              val errors = errorsVec reduceOption { _ ++ _ } getOrElse Set()
              val constr = constrVec reduceOption { _ ++ _ } getOrElse Set()
              
              expr.provenance = NullProvenance
              
              (errors, constr)
            }
          }
        }
        
        case Where(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case With(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        
        case Union(_, left, right) => handleIUI(expr, left, right, relations, constraints)
        case Intersect(_, left, right) => handleIUI(expr, left, right, relations, constraints)
        
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
        
        case Add(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case Sub(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case Mul(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case Div(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case Lt(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case LtEq(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case Gt(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case GtEq(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case Eq(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case NotEq(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case And(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        case Or(_, left, right) => handleBinary(expr, left, right, relations, constraints)
        
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
      
      back
    }
    
    loop(expr, Map(), Map())._1
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
    
    case DynamicDerivedProvenance(source) => {
      val source2 = substituteParam(id, let, source, sub)
      
      source2.cardinality map { cardinality =>
        Stream continually { DynamicProvenance(currentId.getAndIncrement()): Provenance } take cardinality reduceOption UnionProvenance getOrElse ValueProvenance
      } getOrElse DynamicDerivedProvenance(source2)
    }
    
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
      val optRight2 = resolveUnifications(relations)(right)
      
      for {
        left2 <- optLeft2
        right2 <- optRight2
      } yield UnifiedProvenance(left2, right2)
    }
    
    case UnionProvenance(left, right) => {
      val optLeft2 = resolveUnifications(relations)(left)
      val optRight2 = resolveUnifications(relations)(right)
      
      for {
        left2 <- optLeft2
        right2 <- optRight2
      } yield left2 & right2
    }
    
    case DynamicDerivedProvenance(source) =>
      resolveUnifications(relations)(source) map DynamicDerivedProvenance
    
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
    
    // TODO DynamicDerivedProvenance?
    def cardinality: Option[Int] = {
      if (isParametric || this == NullProvenance) {
        None
      } else {
        val back = possibilities filter {
          case ValueProvenance => false
          case _: UnionProvenance => false
          
          // should probably remove UnifiedProvenance, but it's never going to happen
          
          case _ => true
        } size
        
        Some(back)
      }
    }
  }
  
  case class ParamProvenance(id: Identifier, let: ast.Let) extends Provenance {
    override val toString = id.id
    
    val isParametric = true
  }
  
  case class DynamicDerivedProvenance(source: Provenance) extends Provenance {
    override val toString = "@@<" + source.toString + ">"
    
    val isParametric = source.isParametric
    
    override def possibilities = source.possibilities + this
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
  case class SameCard(left: Provenance, right: Provenance) extends ProvConstraint


  private case class ExprWrapper(expr: Expr) {
    override def equals(a: Any): Boolean = a match {
      case ExprWrapper(expr2) => expr equalsIgnoreLoc expr2
      case _ => false
    }

    override def hashCode = expr.hashCodeIgnoreLoc
  }
}
