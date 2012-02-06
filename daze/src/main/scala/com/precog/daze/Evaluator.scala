package com.precog
package daze

import akka.dispatch.{Await, Future}
import akka.util.duration._

import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._
import IterateeT._

import com.precog.analytics.Path
import com.precog.yggdrasil._
import com.precog.util._

trait Evaluator extends DAG with CrossOrdering with OperationsAPI {
  import Function._
  
  import instructions._
  import dag._
  
  def eval[X](graph: DepGraph): DatasetEnum[X, SEvent, IO] = {
    def loop(graph: DepGraph, roots: List[DatasetEnum[X, SEvent, IO]]): Either[DatasetMask[X], DatasetEnum[X, SEvent, IO]] = graph match {
      case SplitRoot(_, depth) => Right(roots(depth))
      
      case Root(_, instr) =>
        Right(ops.point((Vector(), graph.value.get)))    // TODO don't be stupid
      
      case dag.New(_, parent) => loop(parent, roots)
      
      case dag.LoadLocal(_, _, parent, _) => {    // TODO we can do better here
        parent.value match {
          case Some(SString(str)) => Left(query.mask(Path(str)))
          case Some(_) => Right(ops.empty[X, SEvent, IO])
          
          case None => {
            implicit val order = identitiesOrder(parent.provenance.length)
            
            val result = maybeRealize(loop(parent, roots)) flatMap {
              case (_, SString(str)) => Await.result(query.fullProjection[X](Path(str)), 10 seconds)
              case _ => ops.empty[X, SEvent, IO]
            }
            
            Right(ops.sort(result))
          }
        }
      }
      
      case Operate(_, Comp, parent) => {
        val parentRes = loop(parent, roots)
        val parentResTyped = parentRes.left map { _ typed SBoolean }
        val enum = maybeRealize(parentResTyped)
        
        Right(enum collect {
          case (id, SBoolean(b)) => (id, SBoolean(!b))
        })
      }
      
      case Operate(_, Neg, parent) => {
        val parentRes = loop(parent, roots)
        val parentResTyped = parentRes.left map { _ typed SDecimal }
        val enum = maybeRealize(parentResTyped)
        
        Right(enum collect {
          case (id, SDecimal(d)) => (id, SDecimal(-d))
        })
      }
      
      case Operate(_, WrapArray, parent) => {
        val enum = maybeRealize(loop(parent, roots))
        
        Right(enum map {
          case (id, sv) => (id, SArray(Vector(sv)))
        })
      }
      
      case dag.Reduce(_, red, parent) => {
        val enum = maybeRealize(loop(parent, roots))
        
        val mapped = enum map {
          case (_, sv) => sv
        }
        
        val enumP = mapped.enum

        val reducedEnumP: EnumeratorP[X, SValue, IO] = new EnumeratorP[X, SValue, IO] {
          def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[X, SValue, G] = {
            import MO._
            new EnumeratorT[X, SValue, G] {
              def apply[A] = (step: StepT[X, SValue, G, A]) => 
                for {
                  opt <- reductionIter[X, G](red) &= enumP[G]
                  a   <- step.pointI &= (opt.map(sv => EnumeratorT.enumOne[X, SValue, G](sv)).getOrElse(Monoid[EnumeratorT[X, SValue, G]].zero))
                } yield a
            }
          }
        }

        val reducedDS = DatasetEnum[X, SValue, IO](reducedEnumP)
        
        Right(reducedDS map { sv => (Vector(), sv) })
      }
      
      case dag.Split(_, parent, child) => {
        implicit val order = ValuesOrder
        
        val splitEnum = maybeRealize(loop(parent, roots))
        
        val result = ops.sort(splitEnum).uniq flatMap {
          case (_, sv) =>
            maybeRealize(loop(child, ops.point[X, SEvent, IO]((Vector(), sv)) :: roots))
        }
        
        val back: DatasetEnum[X, SEvent, IO] = ops.sort(result).uniq.zipWithIndex map {
          case ((_, sv), id) => (Vector(id), sv)
        }
        
        Right(back)
      }
      
      case Join(_, instr @ (VUnion | VIntersect), left, right) => {
        implicit val sortOfValueOrder = ValuesOrder
        
        val leftEnum = ops.sort(maybeRealize(loop(left, roots)))
        val rightEnum = ops.sort(maybeRealize(loop(right, roots)))
        
        // TODO we're relying on the fact that we *don't* need to preserve sane identities!
        val back = ops.cogroup(leftEnum, rightEnum) collect {
          case Left3(sev)  if instr == VUnion => sev
          case Middle3((sev1, _))             => sev1
          case Right3(sev) if instr == VUnion => sev
        }
        
        Right(back)
      }
      
      // TODO IUnion and IIntersect
      
      case Join(_, Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), left, right) if right.value.isDefined => {
        right.value.get match {
          case sv2 @ SString(str) => {
            val masked = loop(left, roots).left map { _ derefObject str }
            
            masked.right map { enum =>
              enum collect {
                unlift {
                  case (ids, sv1) => binaryOp(DerefObject)(sv1, sv2) map { sv => (ids, sv) }
                }
              }
            }
          }
          
          case _ => Right(ops.empty[X, SEvent, IO])
        }
      }
      
      case Join(_, Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), left, right) if right.value.isDefined => {
        right.value.get match {
          case sv2 @ SDecimal(num) if num.isValidInt => {
            val masked = loop(left, roots).left map { _ derefArray num.toInt }
            
            masked.right map { enum =>
              enum collect {
                unlift {
                  case (ids, sv1) => binaryOp(DerefArray)(sv1, sv2) map { sv => (ids, sv) }
                }
              }
            }
          }
          
          case _ => Right(ops.empty[X, SEvent, IO])
        }
      }
      
      case Join(_, instr, left, right) => {
        lazy val length = sharedPrefixLength(left, right)
        implicit lazy val order = identitiesOrder(length)
        
        val op = instr match {
          case Map2Match(op) => op
          case Map2Cross(op) => op
          case Map2CrossLeft(op) => op
          case Map2CrossRight(op) => op
        }
        
        val leftRes = loop(left, roots)
        val rightRes = loop(right, roots)
        
        val (leftTpe, rightTpe) = binOpType(op)
        
        val leftResTyped = leftRes.left map { mask =>
          leftTpe map mask.typed getOrElse mask
        }
        
        val rightResTyped = rightRes.left map { mask =>
          rightTpe map mask.typed getOrElse mask
        }
        
        val leftEnum = maybeRealize(leftResTyped)
        val rightEnum = maybeRealize(rightResTyped)
        
        val (pairs, distinct) = instr match {
          case Map2Match(op) => (leftEnum join rightEnum, true)
          case Map2Cross(op) => (leftEnum :^ rightEnum, false)
          case Map2CrossLeft(op) => (leftEnum :^ rightEnum, false)
          case Map2CrossRight(op) => (leftEnum ^: rightEnum, false)
        }
        
        val back = pairs collect {
          unlift {
            case ((ids1, sv1), (ids2, sv2)) => {
              val ids = if (distinct)
                (ids1 ++ (ids2 drop length))
              else
                ids1 ++ ids2
              
              binaryOp(op)(sv1, sv2) map { sv => (ids, sv) }
            }
          }
        }
        
        Right(back)
      }
      
      case Filter(_, cross, _, target, boolean) => {
        lazy val length = sharedPrefixLength(target, boolean)
        implicit lazy val order = identitiesOrder(length)
        
        val targetRes = loop(target, roots)
        val booleanRes = loop(boolean, roots)
        
        val booleanResTyped = booleanRes.left map { _ typed SBoolean }
        
        val targetEnum = maybeRealize(targetRes)
        val booleanEnum = maybeRealize(booleanResTyped)
        
        val (pairs, distinct) = cross match {
          case None => (targetEnum join booleanEnum, true)
          case Some(CrossNeutral) => (targetEnum :^ booleanEnum, false)
          case Some(CrossLeft) => (targetEnum :^ booleanEnum, false)
          case Some(CrossRight) => (targetEnum ^: booleanEnum, false)
        }
        
        val back = pairs collect {
          case ((ids1, sv), (ids2, SBoolean(true))) => {
            val ids = if (distinct)
              (ids1 ++ (ids2 drop length))
            else
              ids1 ++ ids2
            
            (ids, sv)
          }
        }
        
        Right(back)
      }
      
      case Sort(parent, indexes) => 
        loop(parent, roots).right map { enum => sortByIdentities(enum, indexes) }
    }
    
    maybeRealize(loop(orderCrosses(graph), Nil))
  }
  
  private def maybeRealize[X](result: Either[DatasetMask[X], DatasetEnum[X, SEvent, IO]]): DatasetEnum[X, SEvent, IO] =
    (result.left map { mask => Await.result(mask.realize, 10 seconds) }).fold(identity, identity)
  
  protected def sortByIdentities[X](enum: DatasetEnum[X, SEvent, IO], indexes: Vector[Int]): DatasetEnum[X, SEvent, IO] = {
    implicit val order: Order[SEvent] = new Order[SEvent] {
      def order(e1: SEvent, e2: SEvent): Ordering = {
        val (ids1, _) = e1
        val (ids2, _) = e2
        
        val left = indexes map ids1
        val right = indexes map ids2
        
        (left zip right).foldLeft[Ordering](Ordering.EQ) {
          case (Ordering.EQ, (i1, i2)) => Ordering.fromInt((i1 - i2) toInt)
          case (acc, _) => acc
        }
      }
    }
    
    ops.sort(enum) map {
      case (ids, sv) => {
        val (first, second) = ids.zipWithIndex partition {
          case (_, i) => indexes contains i
        }
    
        val prefix = first sortWith {
          case ((_, i1), (_, i2)) => indexes.indexOf(i1) < indexes.indexOf(i2)
        }
        
        val (back, _) = (prefix ++ second).unzip
        (back, sv)
      }
    }
  }

  private def unlift[A, B](f: A => Option[B]): PartialFunction[A, B] = new PartialFunction[A, B] {
    def apply(a: A) = f(a).get
    def isDefinedAt(a: A) = f(a).isDefined
  }
  
  // TODO mode, median and stdDev
  private def reductionIter[X, G[_]](red: Reduction)(implicit MO: G |>=| IO): IterateeT[X, SValue, G, Option[SValue]] = {
    import MO._
    red match {
      case Count => {
        fold[X, SValue, G, Option[SValue]](Some(SDecimal(0))) {
          case (Some(SDecimal(acc)), _) => Some(SDecimal(acc + 1))
        }
      }
      
      case Mean => {
        val itr = fold[X, SValue, G, Option[(BigDecimal, BigDecimal)]](None) {
          case (None, SDecimal(v)) => Some((1, v))
          case (Some((count, acc)), SDecimal(v)) => Some((count + 1, acc + v))
          case (acc, _) => acc
        }
        
        itr map {
          case Some((c, v)) => Some(SDecimal(v / c))
          case None => None
        }
      }
      
      case Max => {
        fold[X, SValue, G, Option[SValue]](None) {
          case (None, SDecimal(v)) => Some(SDecimal(v))
          case (Some(SDecimal(v1)), SDecimal(v2)) if v1 >= v2 => Some(SDecimal(v1))
          case (Some(SDecimal(v1)), SDecimal(v2)) if v1 < v2 => Some(SDecimal(v2))
          case (acc, _) => acc
        }
      }
      
      case Min => {
        fold[X, SValue, G, Option[SValue]](None) {
          case (None, SDecimal(v)) => Some(SDecimal(v))
          case (Some(SDecimal(v1)), SDecimal(v2)) if v1 <= v2 => Some(SDecimal(v1))
          case (Some(SDecimal(v1)), SDecimal(v2)) if v1 > v2 => Some(SDecimal(v2))
          case (acc, _) => acc
        }
      }
      
      case StdDev => {
        val itr = fold[X, SValue, G, Option[(BigDecimal, BigDecimal, BigDecimal)]](None) {
          case (None, SDecimal(v)) => Some((1, v, v * v))
          case (Some((count, sum, sumsq)), SDecimal(v)) => Some((count + 1, sum + v, sumsq + (v * v)))
          case (acc, _) => acc
        }
        
        itr map {
          case Some((count, sum, sumsq)) => Some(SDecimal(sqrt(count * sumsq - sum * sum) / count))
          case None => None
        }
      }
      
      case Sum => {
        fold[X, SValue, G, Option[SValue]](None) {
          case (None, sv @ SDecimal(_)) => Some(sv)
          case (Some(SDecimal(acc)), SDecimal(v)) => Some(SDecimal(acc + v))
          case (acc, _) => acc
        }
      }
    }
  }
  
  /**
   * Newton's approximation to some number of iterations (by default: 50).
   * Ported from a Java example found here: http://www.java2s.com/Code/Java/Language-Basics/DemonstrationofhighprecisionarithmeticwiththeBigDoubleclass.htm
   */
  private[this] def sqrt(d: BigDecimal, k: Int = 50): BigDecimal = {
    lazy val approx = {   // could do this with a self map, but it would be much slower
      def gen(x: BigDecimal): Stream[BigDecimal] = {
        val x2 = (d + x * x) / (x * 2)
        
        lazy val tail = if (x2 == x)
          Stream.empty
        else
          gen(x2)
        
        x2 #:: tail
      }
      
      gen(d / 3)
    }
    
    approx take k last
  }
  
  private def binOpType(op: BinaryOperation): (Option[SType], Option[SType]) = op match {
    case Add | Sub | Mul | Div | Lt | LtEq | Gt | GtEq =>
      (Some(SDecimal), Some(SDecimal))
    
    case Eq | NotEq => (None, None)
    
    case Or | And => (Some(SBoolean), Some(SBoolean))
    
    case WrapObject => (Some(SString), None)
    
    case JoinObject => (Some(SObject), Some(SObject))
    
    case JoinArray => (Some(SArray), Some(SArray))
    
    case ArraySwap => (Some(SArray), Some(SDecimal))
    
    case DerefObject => (Some(SObject), Some(SString))
    
    case DerefArray => (Some(SObject), Some(SDecimal))
  }
  
  private def unOpType(op: UnaryOperation): Option[SType] = op match {
    case instructions.New => None
    case Comp => Some(SBoolean)
    case Neg => Some(SDecimal)
    case WrapArray => None
  }

  private def binaryOp(op: BinaryOperation): (SValue, SValue) => Option[SValue] = {
    import Function._
    
    def add(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = Some(left + right)
    def sub(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = Some(left - right)
    def mul(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = Some(left * right)
    
    def div(left: BigDecimal, right: BigDecimal): Option[BigDecimal] = {
      if (right == 0)
        None
      else
        Some(left / right)
    }
    
    def lt(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left < right)
    def lteq(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left <= right)
    def gt(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left > right)
    def gteq(left: BigDecimal, right: BigDecimal): Option[Boolean] = Some(left >= right)
    
    def eq(left: SValue, right: SValue): Option[Boolean] = Some(left == right)
    def neq(left: SValue, right: SValue): Option[Boolean] = Some(left != right)
    
    def and(left: Boolean, right: Boolean): Option[Boolean] = Some(left && right)
    def or(left: Boolean, right: Boolean): Option[Boolean] = Some(left || right)
    
    def joinObject(left: Map[String, SValue], right: Map[String, SValue]) = Some(left ++ right)
    def joinArray(left: Vector[SValue], right: Vector[SValue]) = Some(left ++ right)
    
    def coerceNumerics(pair: (SValue, SValue)): Option[(BigDecimal, BigDecimal)] = pair match {
      case (SDecimal(left), SDecimal(right)) => Some((left, right))
      case _ => None
    }
    
    def coerceBooleans(pair: (SValue, SValue)): Option[(Boolean, Boolean)] = pair match {
      case (SBoolean(left), SBoolean(right)) => Some((left, right))
      case _ => None
    }
    
    def coerceObjects(pair: (SValue, SValue)): Option[(Map[String, SValue], Map[String, SValue])] = pair match {
      case (SObject(left), SObject(right)) => Some((left, right))
      case _ => None
    }
    
    def coerceArrays(pair: (SValue, SValue)): Option[(Vector[SValue], Vector[SValue])] = pair match {
      case (SArray(left), SArray(right)) => Some((left, right))
      case _ => None
    }
    
    op match {
      case Add => untupled((coerceNumerics _) andThen { _ flatMap (add _).tupled map SDecimal })
      case Sub => untupled((coerceNumerics _) andThen { _ flatMap (sub _).tupled map SDecimal })
      case Mul => untupled((coerceNumerics _) andThen { _ flatMap (mul _).tupled map SDecimal })
      case Div => untupled((coerceNumerics _) andThen { _ flatMap (div _).tupled map SDecimal })
      
      case Lt => untupled((coerceNumerics _) andThen { _ flatMap (lt _).tupled map SBoolean })
      case LtEq => untupled((coerceNumerics _) andThen { _ flatMap (lteq _).tupled map SBoolean })
      case Gt => untupled((coerceNumerics _) andThen { _ flatMap (gt _).tupled map SBoolean })
      case GtEq => untupled((coerceNumerics _) andThen { _ flatMap (gteq _).tupled map SBoolean })
      
      case Eq => untupled((eq _).tupled andThen { _ map SBoolean })
      case NotEq => untupled((neq _).tupled andThen { _ map SBoolean })
      
      case And => untupled((coerceBooleans _) andThen { _ flatMap (and _).tupled map SBoolean })
      case Or => untupled((coerceBooleans _) andThen { _ flatMap (or _).tupled map SBoolean })
      
      case WrapObject => {
        case (SString(key), value) => Some(SObject(Map(key -> value)))
        case _ => None
      }
      
      case JoinObject => untupled((coerceObjects _) andThen { _ flatMap (joinObject _).tupled map SObject })
      case JoinArray => untupled((coerceArrays _) andThen { _ flatMap (joinArray _).tupled map SArray })
      
      case ArraySwap => {
        case (SArray(arr), SDecimal(index)) if index.isValidInt => {
          val i = index.toInt
          if (i <= 0 || i >= arr.length) {
            None
          } else {
            val (left, right) = arr splitAt i
            Some(SArray(left.init ++ Vector(right.head, left.last) ++ right.tail))
          }
        }
        
        case _ => None
      }
      
      case DerefObject => {
        case (SObject(obj), SString(key)) => obj get key
        case _ => None
      }
      
      case DerefArray => {
        case (SArray(arr), SDecimal(index)) if index.isValidInt =>
          arr.lift(index.toInt)
        
        case _ => None
      }
    }
  }
  
  private def sharedPrefixLength(left: DepGraph, right: DepGraph): Int =
    left.provenance zip right.provenance takeWhile { case (a, b) => a == b } length

  private def identitiesOrder(prefixLength: Int): Order[SEvent] = new Order[SEvent] {
    def order(e1: SEvent, e2: SEvent) = {
      val (ids1, _) = e1
      val (ids2, _) = e2
      
      (ids1 zip ids2 take prefixLength).foldLeft[Ordering](Ordering.EQ) {
        case (Ordering.EQ, (i1, i2)) => Ordering.fromInt((i1 - i2) toInt)
        case (acc, _) => acc
      }
    }
  }
  
  /**
   * Implements a sort on event values that is ''stable'', but not coherant.
   * In other words, this is sufficient for union, intersect, match, etc, but is
   * not actually going to give you sane answers if you 
   */
  private object ValuesOrder extends Order[SEvent] {
    def order(e1: SEvent, e2: SEvent) = {
      val (_, sv1) = e1
      val (_, sv2) = e2
      
      orderValues(sv1, sv2)
    }
    
    private def orderValues(sv1: SValue, sv2: SValue): Ordering = {
      val to1 = typeOrdinal(sv1)
      val to2 = typeOrdinal(sv2)
      
      if (to1 == to2) {
        (sv1, sv2) match {
          case (SBoolean(b1), SBoolean(b2)) => Ordering.fromInt(boolOrdinal(b1) - boolOrdinal(b2))
          
          case (SLong(l1), SLong(l2)) => {
            if (l1 < l2)
              Ordering.LT
            else if (l1 == l2)
              Ordering.EQ
            else
              Ordering.GT
          }
          
          case (SDouble(d1), SDouble(d2)) => {
            if (d1 < d2)
              Ordering.LT
            else if (d1 == d2)
              Ordering.EQ
            else
              Ordering.GT
          }
          
          case (SDecimal(d1), SDecimal(d2)) => {
            if (d1 < d2)
              Ordering.LT
            else if (d1 == d2)
              Ordering.EQ
            else
              Ordering.GT
          }
          
          case (SString(str1), SString(str2)) => {
            if (str1 < str2)
              Ordering.LT
            else if (str1 == str2)
              Ordering.EQ
            else
              Ordering.GT
          }
          
          case (SArray(arr1), SArray(arr2)) => {
            if (arr1.length < arr2.length) {
              Ordering.LT
            } else if (arr1.length == arr2.length) {
              val orderings = arr1.view zip arr2.view map {
                case (sv1, sv2) => orderValues(sv1, sv2)
              }
              
              (orderings dropWhile (Ordering.EQ ==) headOption) getOrElse Ordering.EQ
            } else {
              Ordering.GT
            }
          }
          
          case (SObject(obj1), SObject(obj2)) => {
            if (obj1.size < obj2.size) {
              Ordering.LT
            } else if (obj1.size == obj2.size) {
              val pairs1 = obj1.toSeq sortWith { case ((k1, _), (k2, _)) => k1 < k2 }
              val pairs2 = obj2.toSeq sortWith { case ((k1, _), (k2, _)) => k1 < k2 }
              
              val comparisons = pairs1 zip pairs2 map {
                case ((k1, _), (k2, _)) if k1 < k2 => Ordering.LT
                case ((k1, _), (k2, _)) if k1 == k2 => Ordering.EQ
                case ((k1, _), (k2, _)) if k1 > k2 => Ordering.GT
              }
              
              val netKeys = (comparisons dropWhile (Ordering.EQ ==) headOption) getOrElse Ordering.EQ
              
              if (netKeys == Ordering.EQ) {
                val comparisons = pairs1 zip pairs2 map {
                  case ((_, v1), (_, v2)) => orderValues(v1, v2)
                }
                
                (comparisons dropWhile (Ordering.EQ ==) headOption) getOrElse Ordering.EQ
              } else {
                netKeys
              }
            } else {
              Ordering.GT
            }
          }
        }
      } else {
        Ordering.fromInt(to1 - to2)
      }
    }
    
    private def typeOrdinal(sv: SValue) = sv match {
      case SBoolean(_) => 0
      case SLong(_) => 1
      case SDouble(_) => 2
      case SDecimal(_) => 3
      case SString(_) => 4
      case SArray(_) => 5
      case SObject(_) => 6
    }
    
    private def boolOrdinal(b: Boolean) = if (b) 1 else 0
  }
}
