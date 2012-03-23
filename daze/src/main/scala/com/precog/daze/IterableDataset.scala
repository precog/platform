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

case class IterableDataset[A](iterable: Iterable[(Identities, A)]) extends Iterable[A] {
  def iterator: Iterator[A] = iterable.map(_._2).iterator
}

trait IterableDatasetOps extends DatasetOps[IterableDataset] {
  implicit def extend[A](d: IterableDataset[A]): DatasetExtensions[IterableDataset, A] = new IterableDatasetExtensions[A](d)

  def empty[A]: IterableDataset[A] = IterableDataset.empty[A]

  def point[A](value: A): IterableDataset[A] = IterableDataset(value)

  def flattenAndIdentify[A](d: IterableDataset[IterableDataset[A]], nextId: => Long, memoId: Int): IterableDataset[A]
}

class IterableDatasetExtensions[A](val value: IterableDataset[A]) extends DatasetExtensions[IterableDataset, A] {
  /*
  def cogroup[B, C](d2: IterableDataset[B])(f: CogroupF[A, B, C])(implicit order: (A, B) => Ordering) = IterableDataset[C](
    new Iterable[(Identities, C)] {
      def iterator = {
        val left  = value.iterable.iterator
        val right = d2.iterable.iterator
        
        new Iterator[(Identities, C)] {
          private var lastLeft: A = null.asInstanceOf[A]
          private var lastRight: B = null.asInstanceOf[B]
          private var state: CogroupState[B] = Step
          private var bufIdx: Int = -1
          private var _next: C = precomputeNext()

          def hasNext: Boolean = _next != null
          def next: C = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext(); temp }

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
          
          @tailrec private def precomputeNext(): C = {
            state match {
              case Step =>
                val leftElement: A  = if (lastLeft  != null) lastLeft  else if (left.hasNext)  left.next  else null.asInstanceOf[A]
                val rightElement: B = if (lastRight != null) lastRight else if (right.hasNext) right.next else null.asInstanceOf[B]

                if (leftElement == null && rightElement == null) {
                  null.asInstanceOf[C]
                } else if (rightElement == null) {
                  lastLeft = null.asInstanceOf[A]
                  if (f.join) precomputeNext() else f.left(leftElement)
                } else if (leftElement == null) {
                  lastRight = null.asInstanceOf[B]
                  if (f.join) precomputeNext() else f.right(rightElement)
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
                      if (f.join) precomputeNext() else f.left(leftElement)

                    case GT =>
                      lastLeft  = leftElement
                      lastRight = null.asInstanceOf[B]
                      if (f.join) precomputeNext() else f.right(rightElement)
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
                      precomputeNext()

                    case GT =>
                      sys.error("inputs on the right-hand side not sorted")
                  }
                } else {
                  state = Step
                  lastLeft = null.asInstanceOf[A]
                  precomputeNext()
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
                      precomputeNext()
                  }
                } else {
                  lastLeft = null.asInstanceOf[A]
                  lastRight = nextRight
                  state = Step
                  precomputeNext()
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
                        precomputeNext()
                    }
                  } else {
                    state = Step
                    lastLeft  = null.asInstanceOf[A]
                    precomputeNext()
                  }
                }
            }
          }
        }
      }
    }
  )
  */

  // join must drop a prefix of identities from d2 up to the shared prefix length
  def join[B, C](d2: IterableDataset[B], sharedPrefixLength: Int)(f: PartialFunction[(A, B), C]): IterableDataset[C] = IterableDataset[C](
    new Iterable[(Identities, C)] {
      type L = (Identities, A)
      type R = (Identities, B)

      def iterator = {
        val left  = value.iterable.iterator
        val right = d2.iterable.iterator
        
        new Iterator[(Identities, C)] {
          private var lastLeft: L = null.asInstanceOf[L]
          private var lastRight: R = null.asInstanceOf[R]
          private var state: CogroupState[R] = Step
          private var bufIdx: Int = -1
          private var _next: (Identities, C) = precomputeNext()

          def hasNext: Boolean = _next != null
          def next: (Identities, C) = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext(); temp }

          private def order(l: L, r: R) = prefixIdentityOrder(l._1, r._1, sharedPrefixLength)

          @tailrec private def bufferRight(leftElement: L, acc: Vector[R]): (R, Vector[R]) = {
            if (right.hasNext) {
              val rightElement = right.next
              order(leftElement, rightElement) match {
                case EQ => bufferRight(leftElement, acc :+ rightElement)
                case LT => (rightElement, acc)
                case GT => sys.error("inputs on the right-hand side not sorted")
              }
            } else (null.asInstanceOf[R], acc)
          }
          
          @tailrec private def precomputeNext(): (Identities, C) = {
            @inline  def fapply(l: L, r: R): (Identities, C) = {
              val tupled = (l._2, r._2)
              if (f.isDefinedAt(tupled)) (l._1 ++ r._1.drop(sharedPrefixLength), f(tupled))
              else precomputeNext()
            }

            state match {
              case Step =>
                val leftElement: L  = if (lastLeft  != null) lastLeft  else if (left.hasNext)  left.next  else null.asInstanceOf[L]
                val rightElement: R = if (lastRight != null) lastRight else if (right.hasNext) right.next else null.asInstanceOf[R]

                if (leftElement == null || rightElement == null) {
                  null.asInstanceOf[(Identities, C)]
                } else {
                  order(leftElement, rightElement) match {
                    case EQ => 
                      lastLeft  = leftElement
                      lastRight = rightElement
                      state = LastEqual
                      fapply(lastLeft, lastRight)

                    case LT => 
                      lastLeft  = null.asInstanceOf[L]
                      lastRight = rightElement
                      precomputeNext() 

                    case GT =>
                      lastLeft  = leftElement
                      lastRight = null.asInstanceOf[R]
                      precomputeNext()
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
                      fapply(lastLeft, rightElement)

                    case LT => 
                      state = RunLeft(rightElement)
                      precomputeNext()

                    case GT =>
                      sys.error("inputs on the right-hand side not sorted")
                  }
                } else {
                  state = Step
                  lastLeft = null.asInstanceOf[L]
                  precomputeNext()
                }

              case RunLeft(nextRight) =>
                // lastLeft is >= lastRight and < nextRight
                if (left.hasNext) {
                  val leftElement = left.next
                  order(leftElement, lastRight) match {
                    case EQ => fapply(leftElement, lastRight)
                    case LT => sys.error("inputs on the left-hand side not sorted")
                    case GT => 
                      lastLeft  = leftElement
                      lastRight = nextRight
                      state = Step
                      precomputeNext()
                  }
                } else {
                  lastLeft = null.asInstanceOf[L]
                  lastRight = nextRight
                  state = Step
                  precomputeNext()
                }
                
              case Cartesian(bufferedRight) => 
                bufIdx += 1
                if (bufIdx < bufferedRight.length) {
                  fapply(lastLeft, bufferedRight(bufIdx))
                } else {
                  if (left.hasNext) {
                    lastLeft = left.next
                    bufIdx = 0
                    order(lastLeft, bufferedRight(bufIdx)) match {
                      case EQ => 
                        fapply(lastLeft, bufferedRight(bufIdx))

                      case LT => 
                        sys.error("inputs on the left-hand side not sorted")

                      case GT => 
                        state = Step
                        precomputeNext()
                    }
                  } else {
                    state = Step
                    lastLeft  = null.asInstanceOf[L]
                    precomputeNext()
                  }
                }
            }
          }
        }
      }
    }
  )

  def crossLeft[B, C](d2: IterableDataset[B])(f: PartialFunction[(A, B), C]): IterableDataset[C] = IterableDataset[C](
    new Iterable[(Identities, C)] {
      type L = (Identities, A)
      type R = (Identities, B)

      def iterator = {
        val left = value.iterable.iterator
        var right: Iterator[R] = null.asInstanceOf[Iterator[R]]

        new Iterator[C] {
          private var leftElement: L = null.asInstanceOf[L]
          private var _next: (Identities, C) = precomputeNext()

          def hasNext = _next != null
          def next: (Identities, C) = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext(); temp }

          @tailrec private def precomputeNext(): (Identities, C) = {
            if (leftElement != null) {
              if (right.hasNext) {
                val (rid, rv) = right.next
                val tupled = (leftElement._2, rv)
                if (f.isDefinedAt(tupled)) {
                  (leftElement._1 ++ rid, f(tupled))
                } else {
                  leftElement = null.asInstanceOf[L]
                  precomputeNext()
                }
              } else {
                leftElement = null.asInstanceOf[L]
                precomputeNext()
              }
            } else if (left.hasNext) {
              leftElement = left.next
              right = d2.iterator
              if (right.hasNext) precomputeNext() else null.asInstanceOf[(Identities, C)]
            } else {
              null.asInstanceOf[(Identities, C)]
            }
          }
        }
      }
    }
  )

  def crossRight[B, C](d2: IterableDataset[B])(f: PartialFunction[(A, B), C]): IterableDataset[C] = 
    new IterableDatasetExtensions(d2).crossLeft(value) { case (er, el) if f.isDefinedAt((el, er)) => f((el, er)) }

  // pad identities to the longest side, then sort -u by identities
  def paddedMerge(d2: Dataset[A], nextId: => Long): Dataset[A]

  def union(d2: IterableDataset[A])(implicit order: Order[A]): IterableDataset[A] = sys.error("todo")

  def intersect(d2: IterableDataset[A])(implicit order: Order[A]): IterableDataset[A] = sys.error("todo")

  def map[B](f: A => B): IterableDataset[B]  = value.iterable.map { case (i, v) => (i, f(v)) }

  def collect[B](pf: PartialFunction[A, B]): IterableDataset[B] = sys.error("todo")

  def reduce[B](base: B)(f: (B, A) => B): B = value.iterator.reduce(f)

  def count: BigInt = value.iterable.size

  // uniq by value, discard identities - assume input not sorted
  def uniq(nextId: => Long)(implicit order: Order[A]): IterableDataset[A] = sys.error("todo")

  // identify(None) strips all identities
  def identify(baseId: Option[() => Long]): IterableDataset[A] = sys.error("todo")

  def sortByIndexedIds(indices: Vector[Int], memoId: Int)(implicit cm: Manifest[A], fs: FileSerialization[A]): IterableDataset[A] = sys.error("todo")
  /*
  protected def sortByIdentities(enum: IterableDataset[SEvent], indexes: Vector[Int], memoId: Int, ctx: MemoizationContext): IterableDataset[SEvent] = {
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

  def memoize(memoId: Int)(implicit fs: FileSerialization[A]): IterableDataset[A]  = sys.error("todo")

  def group[K](memoId: Int)(keyFor: A => K)(implicit ord: Order[K], fs: FileSerialization[A], kvs: FileSerialization[(K, IterableDataset[A])]): IterableDataset[(K, IterableDataset[A])] = sys.error("todo")

  def perform(io: IO[_]): IterableDataset[A] = sys.error("todo")
}

// vim: set ts=4 sw=4 et:
