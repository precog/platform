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
package com.precog.yggdrasil
package iterable

import Buffering._
import serialization._
import memoization._
import com.precog.common.VectorCase
import com.precog.util.{KMap, IdGen}

import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.duration._
import blueeyes.util.Clock

import java.io.File

import scala.annotation.tailrec
import scalaz.{NonEmptyList => NEL, _}
import scalaz.Ordering._
import scalaz.effect._
import scalaz.syntax.semigroup._

sealed trait CogroupState[+ER]
case object Step extends CogroupState[Nothing]
case object LastEqual extends CogroupState[Nothing]
case class RunLeft[+ER](nextRight: ER) extends CogroupState[ER]
case class Cartesian[+ER](bufferedRight: Vector[ER]) extends CogroupState[ER]

case class IterableGrouping[K, A](iterator: Iterator[(K, A)])

trait IterableDatasetOpsConfig {
  def sortBufferSize: Int
  def sortWorkDir: File
  def clock: Clock
}


trait IterableDatasetOpsComponent extends DatasetOpsComponent with YggConfigComponent {
  type YggConfig <: IterableDatasetOpsConfig
  type Dataset[α] = IterableDataset[α]
  type Memoable[α] = Iterable[α]
  type Grouping[K, A] = IterableGrouping[K, A]

  class Ops extends DatasetOps[Dataset, Memoable, Grouping] with GroupingOps[Dataset, Memoable, Grouping]{
    implicit def extend[A](d: IterableDataset[A]): DatasetExtensions[IterableDataset, Iterable, IterableGrouping, A] = 
      new IterableDatasetExtensions[A](d, yggConfig.clock)

    def empty[A](idCount: Int): IterableDataset[A] = IterableDataset(idCount, Iterable.empty[(Identities, A)])

    def point[A](value: A): IterableDataset[A] = IterableDataset(0, Iterable((Identities.Empty, value)))

    def flattenAndIdentify[A](d: IterableDataset[IterableDataset[A]], nextId: () => Identity): IterableDataset[A] = {
      type IA = (Identities, A)
      IterableDataset(
        1,
        new Iterable[IA] {
          def iterator = new Iterator[IA] {
            private var di = d.iterator

            private var _next: IA = precomputeNext()
            private var inner: Iterator[IA] = if (!di.hasNext) null else di.next.iterable.iterator

            def hasNext = _next != null

            def next() = {
              if (_next == null) throw new IllegalStateException("next called on empty iterator.")
              val temp = _next
              _next = precomputeNext()
              temp
            }

            @tailrec private final def precomputeNext(): IA = {
              if ((inner != null) && inner.hasNext) {
                val (_, sv) = inner.next()
                (VectorCase(nextId()), sv)
              } else if (di.hasNext) {
                inner = di.next.iterable.iterator
                precomputeNext()
              } else null
            }
          }
        })
    }

    def mergeGroups[A, K](d1: Grouping[K, Dataset[A]], d2: Grouping[K, Dataset[A]], isUnion: Boolean, memoCtx: MemoizationContext[Iterable])(implicit ord1: Order[A], ord: Order[K], ss: SortSerialization[(Identities, A)]): Grouping[K, Dataset[A]] = {
      val leftIter = d1.iterator
      val rightIter = d2.iterator

      if (isUnion) {
        IterableGrouping(new Iterator[(K, Dataset[A])] {
          var _next: (K, Dataset[A]) = _
          var _left: (K, Dataset[A]) = if (leftIter.hasNext) leftIter.next() else null
          var _right: (K, Dataset[A]) = if (rightIter.hasNext) rightIter.next() else null

          precomputeNext()

          def hasNext = _next != null

          def next() = {
            if (_next == null) throw new IllegalStateException("next called on empty iterator.")
            val back = _next
            precomputeNext()
            back
          }

          private[this] def precomputeNext() {
            if (_left == null && _right == null) {
              _next = null
            } else if (_left != null && _right == null) {
              _next = _left
              _left = leftIter.next
            } else if (_left == null && _right != null) {
              _next = _right
              _right = rightIter.next
            } else {
              ord.order(_left._1, _right._1) match {
                case LT =>
                  _next = _left
                  if (leftIter.hasNext) {
                    _left = leftIter.next
                  } else {
                    _left = null
                  }
                case GT =>
                  _next = _right
                  if (rightIter.hasNext) {
                    _right = rightIter.next
                  } else {
                    _right = null
                  }
                case EQ => {
                  _next = (_left._1, extend(_left._2).union(_right._2, memoCtx))
                  _left = if (leftIter.hasNext) leftIter.next else null
                  _right = if (rightIter.hasNext) rightIter.next else null
                }
              }
            }
          }
        })
      } else {
        IterableGrouping(new Iterator[(K, Dataset[A])] {
          var _next: (K, Dataset[A]) = _
          var _left: (K, Dataset[A]) = if (leftIter.hasNext) leftIter.next() else null
          var _right: (K, Dataset[A]) = if (rightIter.hasNext) rightIter.next() else null

          precomputeNext()

          def hasNext = _next != null

          def next() = {
            if (_next == null) throw new IllegalStateException("next called on empty iterator.")
            val back = _next
            precomputeNext()
            back
          }

          @tailrec private[this] def precomputeNext() {
            if (_left == null || _right == null) {
              _next = null
            } else {
              ord.order(_left._1, _right._1) match {
                case LT =>
                  if (leftIter.hasNext) {
                    _left = leftIter.next; precomputeNext()
                  } else {
                    _next = null
                  }
                case GT =>
                  if (rightIter.hasNext) {
                    _right = rightIter.next; precomputeNext()
                  } else {
                    _next = null
                  }
                case EQ => {
                  _next = (_left._1, extend(_left._2).intersect(_right._2, memoCtx))
                  _left = if (leftIter.hasNext) leftIter.next else null
                  _right = if (rightIter.hasNext) rightIter.next else null
                }
              }
            }
          }
        })
      }
    }

    def zipGroups[A, K: Order](d1: Grouping[K, NEL[Dataset[A]]], d2: Grouping[K, NEL[Dataset[A]]]): Grouping[K, NEL[Dataset[A]]] = {
      val ord = implicitly[Order[K]]

      val leftIter = d1.iterator
      val rightIter = d2.iterator

      IterableGrouping(new Iterator[(K, NEL[Dataset[A]])] {
        var _next: (K, NEL[Dataset[A]]) = _
        var _left: (K, NEL[Dataset[A]]) = if (leftIter.hasNext) leftIter.next() else null
        var _right: (K, NEL[Dataset[A]]) = if (rightIter.hasNext) rightIter.next() else null

        precomputeNext()

        def hasNext = _next != null

        def next() = {
          if (_next == null) throw new IllegalStateException("next called on empty iterator.")
          val back = _next
          precomputeNext()
          back
        }

        @tailrec private[this] def precomputeNext() {
          if (_left == null || _right == null) {
            _next = null
          } else {
            ord.order(_left._1, _right._1) match {
              case LT =>
                if (leftIter.hasNext) {
                  _left = leftIter.next; precomputeNext()
                } else {
                  _next = null
                }
              case GT =>
                if (rightIter.hasNext) {
                  _right = rightIter.next; precomputeNext()
                } else {
                  _next = null
                }
              case EQ => {
                _next = (_left._1, _left._2 |+| _right._2)
                  _left = if (leftIter.hasNext) leftIter.next else null
                _right = if (rightIter.hasNext) rightIter.next else null
              }
            }
          }
        }
      })
    }

    def flattenGroup[A, K, B](g: Grouping[K, NEL[Dataset[A]]], nextId: () => Identity, memoId: Int, memoCtx: MemoizationContext[Iterable])(f: (K, NEL[Dataset[A]]) => Dataset[B])(implicit buffering: Buffering[B], fs: IncrementalSerialization[(Identities, B)]): Dataset[B] = {
      type IB = (Identities, B)

      val gIterator = g.iterator

      val iter = new Iterator[IB] {
        private[this] var _currentIterator: Iterator[IB] = getNextValidIteratorFromG()

        @tailrec private[this] def getNextValidIteratorFromG(): Iterator[IB] = {
          // find the first non-empty result of the function
          if (gIterator.hasNext) {
            val (key, value) = gIterator.next
            val nextIterator = f(key, value).iterable.iterator
            if (nextIterator.hasNext) nextIterator else getNextValidIteratorFromG
          } else {
            null
          } 
        }

        def hasNext = _currentIterator != null && _currentIterator.hasNext

        def next = {
          val tmp = _currentIterator.next

          if (! _currentIterator.hasNext) {
            _currentIterator = getNextValidIteratorFromG()
          }

          tmp
        }
      }

      extend(IterableDataset(1, new Iterable[IB] { def iterator = iter })).identify(Some(nextId)).memoize(memoId, memoCtx)
    }

    def mapGrouping[K, A, B](g: Grouping[K, A])(f: A => B): Grouping[K, B] =
      IterableGrouping[K, B](g.iterator.map { case (k,a) => (k, f(a)) })
  }
}

class IterableDatasetExtensions[A](val value: IterableDataset[A], clock: Clock)
extends DatasetExtensions[IterableDataset, Iterable, IterableGrouping, A] {
  private def extend[T](o: IterableDataset[T]) = new IterableDatasetExtensions[T](o, clock)

  /**
   * Used for event reassembly - match is on identities
   */
  def cogroup[B, C](d2: IterableDataset[B])(f: CogroupF[A, B, C]) = {
    type IB = (Identities, B)
    type IC = (Identities, C)
    assert(value.idCount == d2.idCount)
    assert(value.idCount != 0)
    def order(ia: IA, ib: IB) = fullIdentityOrdering(ia._1, ib._1)

    IterableDataset[C](
      value.idCount,
      new Iterable[IC] {
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
            def next: IC = {
              if (_next == null) throw new IllegalStateException("next called on empty iterator.")
              val temp = _next
              _next = precomputeNext()
              temp
            }

            @tailrec def bufferRight(leftElement: IA, acc: Vector[IB]): (IB, Vector[IB]) = {
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
              state match {
                case Step =>
                  val leftElement: IA  = if (lastLeft  != null) lastLeft  else if (left.hasNext)  left.next  else null.asInstanceOf[IA]
                  val rightElement: IB = if (lastRight != null) lastRight else if (right.hasNext) right.next else null.asInstanceOf[IB]

                  if (leftElement == null && rightElement == null) {
                    null.asInstanceOf[IC]
                  } else if (rightElement == null) {
                    lastLeft = null.asInstanceOf[IA]
                    (leftElement._1, f.left(leftElement._2))
                  } else if (leftElement == null) {
                    lastRight = null.asInstanceOf[IB]
                    (rightElement._1, f.right(rightElement._2))
                  } else {
                    order(leftElement, rightElement) match {
                      case EQ => 
                        lastLeft  = leftElement
                        lastRight = rightElement
                        state = LastEqual
                        (lastLeft._1, f.both(lastLeft._2, lastRight._2))

                      case LT => 
                        lastLeft  = null.asInstanceOf[IA]
                        lastRight = rightElement
                        (leftElement._1, f.left(leftElement._2))

                      case GT =>
                        lastLeft  = leftElement
                        lastRight = null.asInstanceOf[IB]
                        (rightElement._1, f.right(rightElement._2))
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
                        (lastLeft._1, f.both(lastLeft._2, rightElement._2))

                      case LT => 
                        state = RunLeft(rightElement)
                        precomputeNext()

                      case GT =>
                        sys.error("inputs on the right-hand side not sorted")
                    }
                  } else {
                    state = RunLeft(null.asInstanceOf[IB])
                    lastLeft = null.asInstanceOf[IA]
                    precomputeNext()
                  }

                case RunLeft(nextRight) =>
                  // lastLeft is >= lastRight and < nextRight
                  if (left.hasNext) {
                    val leftElement = left.next
                    order(leftElement, lastRight) match {
                      case EQ => (leftElement._1, f.both(leftElement._2, lastRight._2))
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
                    (lastLeft._1, f.both(lastLeft._2, bufferedRight(bufIdx)._2))
                  } else {
                    if (left.hasNext) {
                      lastLeft = left.next
                      bufIdx = 0
                      order(lastLeft, bufferedRight(bufIdx)) match {
                        case EQ => 
                          (lastLeft._1, f.both(lastLeft._2, bufferedRight(bufIdx)._2))

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
            def next: IC = {
              if (_next == null) throw new IllegalStateException("next called on empty iterator.")
              val temp = _next
              _next = precomputeNext()
              temp 
            }

            private def order(l: IA, r: IB) = prefixIdentityOrdering(l._1, r._1, sharedPrefixLength)

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
              state match {
                case Step =>
                  val leftElement: IA  = if (lastLeft  != null) lastLeft  else if (left.hasNext)  left.next  else null.asInstanceOf[IA]
                  val rightElement: IB = if (lastRight != null) lastRight else if (right.hasNext) right.next else null.asInstanceOf[IB]

                  if (leftElement == null || rightElement == null) {
                    // spin out the iterators to get them to clean up their resources. Ugh. //TODO: FIX!!!!!
                    if (leftElement  != null) while (left.hasNext) left.next 
                    if (rightElement != null) while (right.hasNext) right.next 
                    null.asInstanceOf[IC]
                  } else {
                    order(leftElement, rightElement) match {
                      case EQ => 
                        lastLeft  = leftElement
                        lastRight = rightElement
                        state = LastEqual
                        val (lid, lv) = lastLeft
                        val (rid, rv) = lastRight
                        val tupled = (lv, rv)

                        if (f.isDefinedAt(tupled)) (lid ++ rid.drop(sharedPrefixLength), f(tupled))
                        else precomputeNext()

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

                        val (lid, lv) = lastLeft
                        val (rid, rv) = rightElement
                        val tupled = (lv, rv)

                        if (f.isDefinedAt(tupled)) (lid ++ rid.drop(sharedPrefixLength), f(tupled))
                        else precomputeNext()

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
                      case EQ => 
                        val (lid, lv) = leftElement
                        val (rid, rv) = lastRight
                        val tupled = (lv, rv)

                        if (f.isDefinedAt(tupled)) (lid ++ rid.drop(sharedPrefixLength), f(tupled))
                        else precomputeNext()

                      case LT => 
                        sys.error("inputs on the left-hand side not sorted")

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
                    val (lid, lv) = lastLeft
                    val (rid, rv) = bufferedRight(bufIdx)
                    val tupled = (lv, rv)

                    if (f.isDefinedAt(tupled)) (lid ++ rid.drop(sharedPrefixLength), f(tupled))
                    else precomputeNext()
                  } else {
                    if (left.hasNext) {
                      lastLeft = left.next
                      bufIdx = 0
                      order(lastLeft, bufferedRight(bufIdx)) match {
                        case EQ => 
                          val (lid, lv) = lastLeft
                          val (rid, rv) = bufferedRight(bufIdx)
                          val tupled = (lv, rv)

                          if (f.isDefinedAt(tupled)) (lid ++ rid.drop(sharedPrefixLength), f(tupled))
                          else precomputeNext()

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
            def next: IC = {
              if (_next == null) throw new IllegalStateException("next called on empty iterator.")
              val temp = _next
              _next = precomputeNext()
              temp 
            }

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

  def crossRight[B, C](d2: IterableDataset[B])(f: PartialFunction[(A, B), C]): IterableDataset[C] = 
    extend(d2).crossLeft(value) { case (er, el) if f.isDefinedAt((el, er)) => f((el, er)) }

  // pad identities to the longest side, then merge and sort -u by identities
  // this must never be called with datasets where the length of the identities is
  // the same on both sides
  def paddedMerge(d2: IterableDataset[A], nextId: () => Identity): IterableDataset[A] = {
    val (left, right) = if (value.idCount > d2.idCount) (value, d2.padIdsTo(value.idCount, nextId()))
                        else if (value.idCount < d2.idCount) (value.padIdsTo(d2.idCount, nextId()), d2)
                        else sys.error("Cannot supply datasets with the same number of identities to paddedMerge")
    
    val cgf = new CogroupF[A, A, A] {
      def left(a: A) = a
      def both(a: A, b: A) = sys.error("Identities are non-unique in paddedMerge")
      def right(a: A) = a
    }

    extend(left).cogroup(right)(cgf)
  }

  def union(d2: IterableDataset[A], memoCtx: MemoizationContext[Iterable])(implicit ord: Order[A], ss: SortSerialization[IA]): IterableDataset[A] = {
    assert(value.idCount == d2.idCount)

    implicit val order = identityValueOrder[A](IdentitiesOrder)
    val sortedLeft = memoCtx.sort(value.iterable, IdGen.nextInt())
    val sortedRight = memoCtx.sort(d2.iterable, IdGen.nextInt())

    IterableDataset(
      value.idCount,
      new Iterable[IA] {
        def iterator = new Iterator[IA] {
          val leftIter = sortedLeft.iterator
          val rightIter = sortedRight.iterator
          
          var lastLeft: IA = if (leftIter.hasNext) leftIter.next else null
          var lastRight: IA = if (rightIter.hasNext) rightIter.next else null
          var _next: IA = precomputeNext()

          def hasNext = _next != null

          def next(): IA = {
            if (_next == null) throw new IllegalStateException("next called on empty iterator.")
            val temp = _next
            _next = precomputeNext()
            temp
          }

          private[this] def precomputeNext(): IA = {
            if (lastLeft == null && lastRight == null) {
              null
            } else if (lastLeft == null) {
              val tmp = lastRight
              lastRight = if (rightIter.hasNext) rightIter.next else null
              tmp
            } else if (lastRight == null) {
              val tmp = lastLeft
              lastLeft = if (leftIter.hasNext) leftIter.next else null
              tmp
            } else {
              order.order(lastLeft, lastRight) match {
                case EQ => {
                  val tmp = lastLeft
                  lastLeft = if (leftIter.hasNext) leftIter.next() else null
                  lastRight = if (rightIter.hasNext) rightIter.next() else null
                  tmp
                }

                case LT => {
                  val tmp = lastLeft
                  lastLeft = if (leftIter.hasNext) leftIter.next() else null
                  tmp
                }

                case GT => {
                  val tmp = lastRight
                  lastRight = if (rightIter.hasNext) rightIter.next() else null
                  tmp
                }
              } 
            }
          }
        }
      }
    )
  }

  def intersect(d2: IterableDataset[A], memoCtx: MemoizationContext[Iterable])(implicit ord: Order[A], ss: SortSerialization[IA]): IterableDataset[A] =  {
    assert(value.idCount == d2.idCount)

    implicit val order = identityValueOrder[A](IdentitiesOrder)
    val sortedLeftIterable = memoCtx.sort(value.iterable, IdGen.nextInt())
    val sortedRightIterable = memoCtx.sort(d2.iterable, IdGen.nextInt())
    IterableDataset(
      value.idCount,
      new Iterable[IA] {
        def iterator = new Iterator[IA] {
          val sortedLeft = sortedLeftIterable.iterator
          val sortedRight = sortedRightIterable.iterator

          var _left : IA = if (sortedLeft.hasNext) sortedLeft.next else null
          var _right : IA = if (sortedRight.hasNext) sortedRight.next else null
          var _next : IA = precomputeNext()

          def hasNext = _next != null

          def next = {
            if (_next == null) throw new IllegalStateException("next called on empty iterator.")
            val temp = _next
            _next = precomputeNext()
            temp 
          }

          @tailrec private def precomputeNext(): IA = {
            if (_left == null || _right == null) {    // TODO no longer assume full consumption
              while (sortedLeft.hasNext) sortedLeft.next()
              while (sortedRight.hasNext) sortedRight.next()
              null
            } else {
              order.order(_left, _right) match {
                case EQ => 
                  val temp = _left
                  _left = if (sortedLeft.hasNext) sortedLeft.next() else null
                  _right = if (sortedRight.hasNext) sortedRight.next() else null
                  temp

                case LT =>
                  _left = if (sortedLeft.hasNext) sortedLeft.next() else null
                  precomputeNext()

                case GT =>
                  _right = if (sortedRight.hasNext) sortedRight.next() else null
                  precomputeNext()
              }
            }
          }
        }
      }
    )
  }

  def map[B](f: A => B): IterableDataset[B] = IterableDataset(value.idCount, value.iterable.map { case (i, v) => (i, f(v)) })

  def collect[B](pf: PartialFunction[A, B]): IterableDataset[B] = {
    type IB = (Identities, B)
    IterableDataset[B](
      value.idCount,
      new Iterable[IB] {
        def iterator = new Iterator[IB] {
          val iterator = value.iterable.iterator
          private var _next: IB = precomputeNext()

          @tailrec def precomputeNext(): IB = {
            if (iterator.hasNext) {
              val (ids, b) = iterator.next
              if (pf.isDefinedAt(b)) (ids, pf(b)) else precomputeNext()
            } else {
              null.asInstanceOf[IB]
            }
          }

          def hasNext = _next != null
          def next = {
            if (_next == null) throw new IllegalStateException("next called on empty iterator.")
            val tmp = _next
            _next = precomputeNext()
            tmp
          }
        }
      }
    )
  }

  def reduce[B](base: B)(f: (B, A) => B): B = value.iterator.foldLeft(base)(f)

  def count: BigInt = value.iterable.size

  def uniq(nextId: () => Identity, memoId: Int, memoCtx: MemoizationContext[Iterable])(implicit buffering: Buffering[A], fs: SortSerialization[A]): IterableDataset[A] = {
    val sorted = memoCtx.sort(new Iterable[A] { def iterator = value.iterator }, memoId)

    IterableDataset[A](1, 
      new Iterable[IA] {
        def iterator: Iterator[IA] = new Iterator[IA]{
          val inner = sorted.iterator
          private var atStart = true
          private var _next: A = precomputeNext()

          @tailrec private def precomputeNext(): A = {
            if (inner.hasNext) {
              if (atStart) {
                atStart = false
                inner.next
              } else {
                val tmp = inner.next
                if (buffering.order.order(_next, tmp) == EQ) precomputeNext() else tmp
              }
            } else {
              null.asInstanceOf[A]
            }
          }

          def hasNext = _next != null
          def next = {
            if (_next == null) throw new IllegalStateException("next called on empty iterator.")
            val result = (VectorCase(nextId()), _next)
            _next = precomputeNext()
            result
          }
        }
      }
    )
  }

  // identify(None) strips all identities
  def identify(baseId: Option[() => Identity]): IterableDataset[A] = {
    baseId match {
      case Some(newId) => {
        IterableDataset(
          1,
          new Iterable[IA] {
            def iterator = new Iterator[IA] {
              val inner = value.iterator

              def hasNext = inner.hasNext

              def next = {
                val sv = inner.next
                (VectorCase(newId()), sv)
              }
            }
          })
      }

      case None => {
        val emptyVector = VectorCase()
        IterableDataset(
          1,
          new Iterable[IA] {
            def iterator = new Iterator[IA] {
              val inner = value.iterator

              def hasNext = inner.hasNext

              def next = {
                val sv = inner.next
                (emptyVector, sv)
              }
            }
          })
      }
        
    }
  }

  def sortByIndexedIds(indices: Vector[Int], memoId: Int, memoCtx: MemoizationContext[Iterable])(implicit fs: SortSerialization[IA]): IterableDataset[A] = {
    val remappedIds = indices.toSet
    def remapIndices(ids: Identities): Identities = {
      val excluded = (0 until ids.length).filterNot(remappedIds)
      VectorCase(indices.map(ids) ++ excluded.map(ids): _*)
    }

    val remapped = value.iterable map { case (ids, value) => (remapIndices(ids), value) }
    implicit val order = tupledIdentitiesOrder[A](prefixIdentityOrder(indices.length))
    IterableDataset(value.idCount, memoCtx.sort(remapped, memoId))
  }

  def sortByValue(memoId: Int, memoCtx: MemoizationContext[Iterable])(implicit ord: Order[A], fs: SortSerialization[IA]): IterableDataset[A] = {
    implicit val order = valueOrder[A]
    implicit val buffering = Buffering.refBuffering[(Identities, A)](order, implicitly[ClassManifest[(Identities, A)]])

    IterableDataset(value.idCount, memoCtx.sort(value.iterable, memoId))
  }

  def sortByIdentity(memoId: Int, memoCtx: MemoizationContext[Iterable])(implicit fs: SortSerialization[IA]): IterableDataset[A] = {
    implicit val order = tupledIdentitiesOrder[A](IdentitiesOrder)

    IterableDataset(value.idCount, memoCtx.sort(value.iterable, memoId))
  }

  def memoize(memoId: Int, memoCtx: MemoizationContext[Iterable])(implicit serialization: IncrementalSerialization[(Identities, A)]): IterableDataset[A] = {
    IterableDataset(value.idCount, memoCtx.memoize(value.iterable, memoId))
  }

  def group[K](memoId: Int, memoCtx: MemoizationContext[Iterable])(keyFor: A => IterableDataset[K])(implicit ord: Order[K], kvs: SortSerialization[(K, Identities, A)], ms: IncrementalSerialization[(Identities, A)]): IterableGrouping[K, IterableDataset[A]] = {
    implicit val kaOrder: Order[(K,Identities,A)] = new Order[(K,Identities,A)] {
      def order(x: (K,Identities,A), y: (K,Identities,A)) = {
        val kOrder = ord.order(x._1, y._1)
        if (kOrder == Ordering.EQ) {
          IdentitiesOrder.order(x._2, y._2)
        } else kOrder
      }
    }

    val withK = new Iterable[(K, Identities, A)] {
      def iterator = value.iterable.iterator.flatMap { 
        case (i, a) => keyFor(a).iterator map { k => (k, i, a) }
      }
    }

    IterableGrouping(
      new Iterator[(K, IterableDataset[A])] {
        val sorted = memoCtx.sort(withK, memoId).iterator

        // Have to hold our "peek" over the entire outer iterator
        var _next: (K, Identities, A) = precomputeNext()
        var sameK = true

        def hasNext = _next != null
        def next: (K, IterableDataset[A]) = {  
          if (_next == null) throw new IllegalStateException("next called on empty iterator.")
          val currentK = _next._1
          sameK = true
          
          // Compute an IterableDataset that will run over sorted for all == K
          val innerDS = IterableDataset[A](
            value.idCount, 
            new Iterable[(Identities,A)] {
              def iterator = new Iterator[(Identities,A)] {
                def hasNext = _next != null && sameK
                def next = {
                  if (_next == null) throw new IllegalStateException("next called on an empty iterator.")
                  val tmp = _next
                  _next = precomputeNext()
                  sameK = if (_next == null) false else ord.order(currentK, _next._1) == EQ
                  (tmp._2, tmp._3)
                }
              }
            }
          )

          (currentK, extend(innerDS).memoize(IdGen.nextInt(), memoCtx))
        }

        private def precomputeNext(): (K, Identities, A) = {
          if (sorted.hasNext) sorted.next
          else null.asInstanceOf[(K, Identities, A)]
        }
      }
    )
  }
}
