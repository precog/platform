package com.precog
package daze

import yggdrasil.FileSerialization
import scala.annotation.tailrec
import scalaz._
import scalaz.Ordering._
import scalaz.effect._

sealed trait CogroupState[+ER]
case object Step extends CogroupState[Nothing]
case object LastEqual extends CogroupState[Nothing]
case class RunLeft[+ER](nextRight: ER) extends CogroupState[ER]
case class Cartesian[+ER](bufferedRight: Vector[ER]) extends CogroupState[ER]

trait IterableDatasetOps extends DatasetOps[Iterable] {
  implicit def extend[A](d: Iterable[A]): DatasetExtensions[Iterable, A] = new IterableDatasetExtensions[A](d)

  def empty[A]: Iterable[A] = Iterable.empty[A]

  def point[A](value: A): Iterable[A] = Iterable(value)
}

class IterableDatasetExtensions[A](val value: Iterable[A]) extends DatasetExtensions[Iterable, A] {
  def cogroup[B, C](d2: Iterable[B])(f: CogroupF[A, B, C])(implicit order: (A, B) => Ordering): Iterable[C] = 
    new Iterable[C] {
      def iterator = {
        val left  = value.iterator
        val right = d2.iterator
        
        new Iterator[C] {
          private var lastLeft: A = null.asInstanceOf[A]
          private var lastRight: B = null.asInstanceOf[B]
          private var state: CogroupState[B] = Step
          private var bufIdx: Int = -1
          private var _next: C = precomputeNext

          def hasNext: Boolean = _next != null
          def next: C = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext; temp }

          @tailrec def bufferRight(leftElement: A, acc: Vector[B]): (B, Vector[B]) = {
            if (right.hasNext) {
              val rightElement = right.next
              order(leftElement, rightElement) match {
                case EQ => bufferRight(leftElement, acc :+ rightElement)
                case LT => (rightElement, acc)
                case GT => sys.error("inputs on the right-hand side not sorted")
              }
            } else (null.asInstanceOf[B], acc)
          }
          
          @tailrec private def precomputeNext: C = {
            state match {
              case Step =>
                val leftElement: A  = if (lastLeft  != null) lastLeft  else if (left.hasNext)  left.next  else null.asInstanceOf[A]
                val rightElement: B = if (lastRight != null) lastRight else if (right.hasNext) right.next else null.asInstanceOf[B]

                if (leftElement == null && rightElement == null) {
                  null.asInstanceOf[C]
                } else if (rightElement == null) {
                  lastLeft = null.asInstanceOf[A]
                  if (f.join) precomputeNext else f.left(leftElement)
                } else if (leftElement == null) {
                  lastRight = null.asInstanceOf[B]
                  if (f.join) precomputeNext else f.right(rightElement)
                } else {
                  order(leftElement, rightElement) match {
                    case EQ => 
                      lastLeft  = leftElement
                      lastRight = rightElement
                      state = LastEqual
                      f.both(lastLeft, lastRight)

                    case LT => 
                      lastLeft  = null.asInstanceOf[A]
                      lastRight = rightElement
                      if (f.join) precomputeNext else f.left(leftElement)

                    case GT =>
                      lastLeft  = leftElement
                      lastRight = null.asInstanceOf[B]
                      if (f.join) precomputeNext else f.right(rightElement)
                  }
                } 
              
              case LastEqual =>
                if (right.hasNext) {
                  val rightElement = right.next
                  order(lastLeft, rightElement) match {
                    case EQ => 
                      // we have a run of at least two on the right, so just buffer and then drop into 
                      // the cartesian state
                      val (nextRight, bufferedRight) = bufferRight(lastLeft, Vector(lastRight, rightElement))
                      // store the first element on the right beyond the end of the run; this reference will
                      // not be modified until in the Step state again
                      lastRight = nextRight
                      state = Cartesian(bufferedRight)
                      bufIdx = 1 // bufIdx will be incremented before the buffer is read
                      f.both(lastLeft, rightElement)

                    case LT => 
                      state = RunLeft(rightElement)
                      precomputeNext

                    case GT =>
                      sys.error("inputs on the right-hand side not sorted")
                  }
                } else {
                  state = Step
                  lastLeft = null.asInstanceOf[A]
                  precomputeNext
                }

              case RunLeft(nextRight) =>
                // lastLeft is >= lastRight and < nextRight
                if (left.hasNext) {
                  val leftElement = left.next
                  order(leftElement, lastRight) match {
                    case EQ => f.both(leftElement, lastRight)
                    case LT => sys.error("inputs on the left-hand side not sorted")
                    case GT => 
                      lastLeft  = leftElement
                      lastRight = nextRight
                      state = Step
                      precomputeNext
                  }
                } else {
                  lastLeft = null.asInstanceOf[A]
                  lastRight = nextRight
                  state = Step
                  precomputeNext
                }
                
              case Cartesian(bufferedRight) => 
                bufIdx += 1
                if (bufIdx < bufferedRight.length) {
                  f.both(lastLeft, bufferedRight(bufIdx))
                } else {
                  if (left.hasNext) {
                    lastLeft = left.next
                    bufIdx = 0
                    order(lastLeft, bufferedRight(bufIdx)) match {
                      case EQ => 
                        f.both(lastLeft, bufferedRight(bufIdx))

                      case LT => 
                        sys.error("inputs on the left-hand side not sorted")

                      case GT => 
                        state = Step
                        precomputeNext
                    }
                  } else {
                    state = Step
                    lastLeft  = null.asInstanceOf[A]
                    precomputeNext
                  }
                }
            }
          }
        }
      }
    }

  // join must drop a prefix of identities from d2 up to the shared prefix length
  def join[B, C](d2: Iterable[B], sharedPrefixLength: Int)(f: PartialFunction[(A, B), C]): Iterable[C] = sys.error("todo")


  def crossLeft[B, C](d2: Iterable[B])(f: PartialFunction[(A, B), C]): Iterable[C] = 
    new Iterable[C] {
      def iterator = {
        val left = value.iterator
        var right: Iterator[B] = null.asInstanceOf[Iterator[B]]

        new Iterator[C] {
          private var leftElement: A = null.asInstanceOf[A]
          private var _next: C = precomputeNext

          def hasNext = _next != null
          def next: C = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext; temp }

          @tailrec private def precomputeNext: C = {
            if (leftElement != null) {
              if (right.hasNext) {
                val tupled = (leftElement, right.next)
                if (f.isDefinedAt(tupled)) {
                  f(tupled)
                } else {
                  leftElement = null.asInstanceOf[A]
                  precomputeNext
                }
              } else {
                leftElement = null.asInstanceOf[A]
                precomputeNext
              }
            } else if (left.hasNext) {
              leftElement = left.next
              right = d2.iterator
              if (right.hasNext) precomputeNext else null.asInstanceOf[C]
            } else {
              null.asInstanceOf[C]
            }
          }
        }
      }
    }

  def crossRight[B, C](d2: Iterable[B])(f: PartialFunction[(A, B), C]): Iterable[C] = 
    new IterableDatasetExtensions(d2).crossLeft(value) { case (er, el) if f.isDefinedAt((el, er)) => f((el, er)) }

  // pad identities to the longest side on identity union
  // value union discards identities
  def union(d2: Iterable[A], idUnion: Boolean): Iterable[A] = sys.error("todo")

  // value intersection discards identities
  def intersect(d2: Iterable[A], idIntersect: Boolean): Iterable[A] = sys.error("todo")

  def merge(d2: Iterable[A])(implicit order: Order[A]): Iterable[A] = sys.error("todo")

  def map[B](f: A => B): Iterable[B]  = sys.error("todo")

  def flatMap[B](f: A => Iterable[B]): Iterable[B] = sys.error("todo")

  def collect[B](pf: PartialFunction[A, B]): Iterable[B] = sys.error("todo")

  def reduce[B](base: B)(f: (B, A) => B): B = sys.error("todo")

  def count: BigInt = sys.error("todo")

  // uniq by value, discard identities - assume input not sorted
  def uniq: Iterable[A] = sys.error("todo")

  // identify(None) strips all identities
  def identify(baseId: Option[() => Long]): Iterable[A] = sys.error("todo")

  def sortByIds(memoId: Int)(cm: Manifest[A], fs: FileSerialization[A]): Iterable[A] = sys.error("todo")
  def sortByIndexedIds(indices: Vector[Int], memoId: Int)(implicit cm: Manifest[A], fs: FileSerialization[A]): Iterable[A] = sys.error("todo")
  /*
  protected def sortByIdentities(enum: Iterable[SEvent], indexes: Vector[Int], memoId: Int, ctx: MemoizationContext): Iterable[SEvent] = {
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
    
    ops.sort(enum, Some((memoId, ctx))) map {
      case (ids, sv) => {
        val (first, second) = ids.zipWithIndex partition {
          case (_, i) => indexes contains i
        }
    
        val prefix = first sortWith {
          case ((_, i1), (_, i2)) => indexes.indexOf(i1) < indexes.indexOf(i2)
        }
        
        val (back, _) = (prefix ++ second).unzip
        (VectorCase.fromSeq(back), sv)
      }
    }
  }
  */

  def sortByValue(memoId: Int)(implicit order: Order[A], cm: Manifest[A], fs: FileSerialization[A]): Iterable[A]  = sys.error("todo")
  
  def memoize(memoId: Int)(implicit fs: FileSerialization[A]): Iterable[A]  = sys.error("todo")

  def group[K](memoId: Int)(keyFor: A => K)(implicit ord: Order[K], fs: FileSerialization[A], kvs: FileSerialization[(K, Iterable[A])]): Iterable[(K, Iterable[A])] = sys.error("todo")

  def perform(io: IO[_]): Iterable[A] = sys.error("todo")
}

// vim: set ts=4 sw=4 et:
