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
package com.precog
package daze

import com.precog.yggdrasil._
import com.precog.common.VectorCase
import com.precog.util._

import scala.annotation.tailrec
import scalaz.{NonEmptyList => NEL, Identity => _, _}
import scalaz.Ordering._
import scalaz.effect._

sealed trait CogroupState[+ER]
case object Step extends CogroupState[Nothing]
case object LastEqual extends CogroupState[Nothing]
case class RunLeft[+ER](nextRight: ER) extends CogroupState[ER]
case class Cartesian[+ER](bufferedRight: Vector[ER]) extends CogroupState[ER]

case class IterableGrouping[K, A](iterable: Iterable[(K, A)]) 

trait IterableDatasetOpsComponent extends YggConfigComponent {
  type YggConfig <: SortConfig

  trait IterableDatasetOps extends DatasetOps[IterableDataset, IterableGrouping] {
    implicit def extend[A <: AnyRef](d: IterableDataset[A]): DatasetExtensions[IterableDataset, IterableGrouping, A] = 
      new IterableDatasetExtensions[A](d, new IteratorSorting(yggConfig))

    def empty[A](idCount: Int): IterableDataset[A] = IterableDataset(idCount, Iterable.empty[(Identities, A)])

    def point[A](value: A): IterableDataset[A] = IterableDataset(0, Iterable((Identities.Empty, value)))

    def flattenAndIdentify[A](d: IterableDataset[IterableDataset[A]], nextId: => Long, memoId: Int): IterableDataset[A]
  }
}

class IterableDatasetExtensions[A <: AnyRef](val value: IterableDataset[A], iteratorSorting: Sorting[Iterator]) 
extends DatasetExtensions[IterableDataset, IterableGrouping, A] {
  type IA = (Identities, A)

  // join must drop a prefix of identities from d2 up to the shared prefix length
  def join[B, C](d2: IterableDataset[B], sharedPrefixLength: Int)(f: PartialFunction[(A, B), C]): IterableDataset[C] = {
    type IC = (Identities, C)
    IterableDataset[C](
      value.idCount + d2.idCount - sharedPrefixLength,
      new Iterable[IC] {
        type IB = (Identities, B)

        def iterator = {
          val left  = value.iterable.iterator
          val right = d2.iterable.iterator
          
          new Iterator[IC] {
            private var lastLeft: IA = null.asInstanceOf[IA]
            private var lastRight: IB = null.asInstanceOf[IB]
            private var state: CogroupState[IB] = Step
            private var bufIdx: Int = -1
            private var _next: IC = precomputeNext()

            def hasNext: Boolean = _next != null
            def next: IC = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext(); temp }

            private def order(l: IA, r: IB) = prefixIdentityOrder(l._1, r._1, sharedPrefixLength)

            @tailrec private def bufferRight(leftElement: IA, acc: Vector[IB]): (IB, Vector[IB]) = {
              if (right.hasNext) {
                val rightElement = right.next
                order(leftElement, rightElement) match {
                  case EQ => bufferRight(leftElement, acc :+ rightElement)
                  case LT => (rightElement, acc)
                  case GT => sys.error("inputs on the right-hand side not sorted")
                }
              } else (null.asInstanceOf[IB], acc)
            }
            
            @tailrec private def precomputeNext(): IC = {
              @inline  def fapply(l: IA, r: IB): IC = {
                val tupled = (l._2, r._2)
                if (f.isDefinedAt(tupled)) (l._1 ++ r._1.drop(sharedPrefixLength), f(tupled))
                else precomputeNext()
              }

              state match {
                case Step =>
                  val leftElement: IA  = if (lastLeft  != null) lastLeft  else if (left.hasNext)  left.next  else null.asInstanceOf[IA]
                  val rightElement: IB = if (lastRight != null) lastRight else if (right.hasNext) right.next else null.asInstanceOf[IB]

                  if (leftElement == null || rightElement == null) {
                    null.asInstanceOf[IC]
                  } else {
                    order(leftElement, rightElement) match {
                      case EQ => 
                        lastLeft  = leftElement
                        lastRight = rightElement
                        state = LastEqual
                        fapply(lastLeft, lastRight)

                      case LT => 
                        lastLeft  = null.asInstanceOf[IA]
                        lastRight = rightElement
                        precomputeNext() 

                      case GT =>
                        lastLeft  = leftElement
                        lastRight = null.asInstanceOf[IB]
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
                    lastLeft = null.asInstanceOf[IA]
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
                    lastLeft = null.asInstanceOf[IA]
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
                      lastLeft  = null.asInstanceOf[IA]
                      precomputeNext()
                    }
                  }
              }
            }
          }
        }
      }
    )
  }

  def crossLeft[B, C](d2: IterableDataset[B])(f: PartialFunction[(A, B), C]): IterableDataset[C] = {
    type IC = (Identities, C)
    IterableDataset[C](
      value.idCount + d2.idCount,
      new Iterable[IC] {
        type IB = (Identities, B)

        def iterator = {
          val left = value.iterable.iterator
          var right: Iterator[IB] = null.asInstanceOf[Iterator[IB]]

          new Iterator[IC] {
            private var leftElement: IA = null.asInstanceOf[IA]
            private var _next: IC = precomputeNext()

            def hasNext = _next != null
            def next: IC = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext(); temp }

            @tailrec private def precomputeNext(): IC = {
              if (leftElement != null) {
                if (right.hasNext) {
                  val (rid, rv) = right.next
                  val tupled = (leftElement._2, rv)
                  if (f.isDefinedAt(tupled)) {
                    (leftElement._1 ++ rid, f(tupled))
                  } else {
                    leftElement = null.asInstanceOf[IA]
                    precomputeNext()
                  }
                } else {
                  leftElement = null.asInstanceOf[IA]
                  precomputeNext()
                }
              } else if (left.hasNext) {
                leftElement = left.next
                right = d2.iterable.iterator
                if (right.hasNext) precomputeNext() else null.asInstanceOf[IC]
              } else {
                null.asInstanceOf[IC]
              }
            }
          }
        }
      }
    )
  }

  def crossRight[B <: AnyRef, C](d2: IterableDataset[B])(f: PartialFunction[(A, B), C]): IterableDataset[C] = 
    new IterableDatasetExtensions(d2, iteratorSorting).crossLeft(value) { case (er, el) if f.isDefinedAt((el, er)) => f((el, er)) }

  // pad identities to the longest side, then merge and sort -u by identities
  def paddedMerge(d2: IterableDataset[A], nextId: () => Identity, memoId: Int)(implicit fs: FileSerialization[IA]): IterableDataset[A] = {
    val (left, right) = if (value.idCount > d2.idCount) (value, d2.padIdsTo(value.idCount, nextId()))
                        else if (value.idCount < d2.idCount) (value.padIdsTo(d2.idCount, nextId()), d2)
                        else (value, d2)
    
    //left merge right
    sys.error("todo")
  }

  def union(d2: IterableDataset[A])(implicit order: Order[A]): IterableDataset[A] = sys.error("todo")

  def intersect(d2: IterableDataset[A])(implicit order: Order[A]): IterableDataset[A] = sys.error("todo")

  def map[B](f: A => B): IterableDataset[B] = value.map(f)

  def collect[B](pf: PartialFunction[A, B]): IterableDataset[B] = sys.error("todo")

  def reduce[B](base: B)(f: (B, A) => B): B = value.iterator.foldLeft(base)(f)

  def count: BigInt = value.iterable.size

  def uniq(nextId: () => Identity, memoId: Int, memoCtx: MemoizationContext)(implicit order: Order[A], cm: ClassManifest[A], fs: SortSerialization[A]): IterableDataset[A] = {
    val filePrefix = "uniq"

    IterableDataset[A](1, 
      new Iterable[IA] {
        def iterator: Iterator[IA] = new Iterator[IA]{
          val sorted = iteratorSorting.sort(value.iterator, filePrefix, memoId, memoCtx)

          def hasNext = sorted.hasNext
          def next = (VectorCase(nextId()), sorted.next)
        }
      }
    )
  }

  // identify(None) strips all identities
  def identify(baseId: Option[() => Identity]): IterableDataset[A] = sys.error("todo")

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
// vim: set ts=4 sw=4 et:
