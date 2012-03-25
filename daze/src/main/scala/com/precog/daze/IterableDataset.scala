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

import yggdrasil._
import yggdrasil.serialization._
import memoization._
import com.precog.common.VectorCase
import com.precog.util._

import scala.annotation.tailrec
import scalaz.{NonEmptyList => NEL, Identity => _, _}
import scalaz.Ordering._
import scalaz.effect._
import scalaz.syntax.semigroup._

sealed trait CogroupState[+ER]
case object Step extends CogroupState[Nothing]
case object LastEqual extends CogroupState[Nothing]
case class RunLeft[+ER](nextRight: ER) extends CogroupState[ER]
case class Cartesian[+ER](bufferedRight: Vector[ER]) extends CogroupState[ER]

case class IterableGrouping[K, A](iterator: Iterator[(K, A)])

trait IterableDatasetOpsComponent extends DatasetOpsComponent with YggConfigComponent {
  type YggConfig <: SortConfig
  type Dataset[E] = IterableDataset[E]
  type Grouping[K, A] = IterableGrouping[K, A]

  trait Ops extends DatasetOps[Dataset, Grouping] with GroupingOps[Dataset, Grouping]{
    implicit def extend[A <: AnyRef](d: IterableDataset[A]): DatasetExtensions[IterableDataset, IterableGrouping, A] = 
      new IterableDatasetExtensions[A](d, new IteratorSorting(yggConfig))

    def empty[A](idCount: Int): IterableDataset[A] = IterableDataset(idCount, Iterable.empty[(Identities, A)])

    def point[A](value: A): IterableDataset[A] = IterableDataset(0, Iterable((Identities.Empty, value)))

    def flattenAndIdentify[A](d: IterableDataset[IterableDataset[A]], nextId: => Long, memoId: Int): IterableDataset[A] = {
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
              val temp = _next

              _next = precomputeNext()

              temp
            }

            @tailrec private final def precomputeNext(): IA = {
              if (inner.hasNext) {
                val (_, sv) = inner.next()
                (VectorCase(nextId), sv)
              } else if (di.hasNext) {
                inner = di.next.iterable.iterator
                precomputeNext()
              } else null
            }
          }
        })
    }

    def mergeGroups[A <: AnyRef, K](d1: Grouping[K, Dataset[A]], d2: Grouping[K, Dataset[A]], isUnion: Boolean)(implicit ord1: Order[A], ord: Order[K], ss: SortSerialization[(Identities, A)]): Grouping[K, Dataset[A]] = {
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
            val back = _next
            precomputeNext()
            back
          }

          private[this] def precomputeNext() {
            if (!leftIter.hasNext && !rightIter.hasNext) {
              _next = null
            } else if (leftIter.hasNext && !rightIter.hasNext) {
              _left = leftIter.next
              _next = _left
            } else if (!leftIter.hasNext && rightIter.hasNext) {
              _right = rightIter.next
              _next = _right
            } else {
              ord.order(_left._1, _right._1) match {
                case LT =>
                  _next = _left
                  if (leftIter.hasNext) {
                    _left = leftIter.next
                  }
                case GT =>
                  _next = _left
                  if (rightIter.hasNext) {
                    _right = rightIter.next
                  }
                case EQ => {
                  _next = (_left._1, extend(_left._2).union(_right._2))
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
            val back = _next
            precomputeNext()
            back
          }

          @tailrec private[this] def precomputeNext() {
            if (!leftIter.hasNext || !rightIter.hasNext) {
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
                  _next = (_left._1, extend(_left._2).intersect(_right._2))
                  _left = if (leftIter.hasNext) leftIter.next else null
                  _right = if (rightIter.hasNext) rightIter.next else null
                }
              }
            }
          }
        })
      }
    }

    def zipGroups[A <: AnyRef, K: Order](d1: Grouping[K, NEL[Dataset[A]]], d2: Grouping[K, NEL[Dataset[A]]]): Grouping[K, NEL[Dataset[A]]] = {
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
          val back = _next
          precomputeNext()
          back
        }

        @tailrec private[this] def precomputeNext() {
          if (!leftIter.hasNext || !rightIter.hasNext) {
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

    def flattenGroup[A <: AnyRef, K, B: Order](g: Grouping[K, NEL[Dataset[A]]], nextId: () => Identity)(f: (K, NEL[Dataset[A]]) => Dataset[B]): Dataset[B] = sys.error("todo")

    def mapGrouping[K, A <: AnyRef, B <: AnyRef](g: Grouping[K, A])(f: A => B): Grouping[K, B] = {
      IterableGrouping[K, B](g.iterator.map { case (k,a) => (k, f(a)) })
    }
  }
}

class IterableDatasetExtensions[A <: AnyRef](val value: IterableDataset[A], iteratorSorting: Sorting[Iterator, Iterable])
extends DatasetExtensions[IterableDataset, IterableGrouping, A] {
  private def extend(o: IterableDataset[A]) = new IterableDatasetExtensions(o, iteratorSorting)

  /**
   * Used for event reassembly - match is on identities
   */
  def cogroup[B, C](d2: IterableDataset[B])(f: CogroupF[A, B, C]) = {
    type IB = (Identities, B)
    type IC = (Identities, C)
    assert(value.idCount == d2.idCount)
    def order(ia: IA, ib: IB) = identityOrder(ia._1, ib._1)

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
            def next: IC = if (_next == null) sys.error("No more") else { val temp = _next; _next = precomputeNext(); temp }

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
                    state = Step
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
  def paddedMerge(d2: IterableDataset[A], nextId: () => Identity, memoId: Int)(implicit fs: SortSerialization[A]): IterableDataset[A] = {
    val (left, right) = if (value.idCount > d2.idCount) (value, d2.padIdsTo(value.idCount, nextId()))
                        else if (value.idCount < d2.idCount) (value.padIdsTo(d2.idCount, nextId()), d2)
                        else (value, d2)
    
    val cgf = new CogroupF[A, A, A] {
      def left(a: A) = a
      def both(a: A, b: A) = sys.error("Identities are non-unique in paddedMerge")
      def right(a: A) = a
    }

    //left merge right
    cogroup(right)(cgf)
  }

  private implicit def orderIA(implicit ord: Order[A]): Order[IA] = new Order[IA] {
    def order(x: IA, y: IA): Ordering = {
      val idComp = IdentitiesOrder.order(x._1, y._1)
      if (idComp == EQ) {
        ord.order(x._2, y._2)
      } else idComp
    }
  }

  private final val nullIA = null.asInstanceOf[IA]

  def union(d2: IterableDataset[A])(implicit ord: Order[A], ss: SortSerialization[IA]): IterableDataset[A] = {
    val order = orderIA(ord)
    val sortedLeft = iteratorSorting.sort(value.iterable.iterator, "union", IdGen.nextInt())
    val sortedRight = iteratorSorting.sort(d2.iterable.iterator, "union", IdGen.nextInt())

    IterableDataset(
      value.idCount,
      new Iterable[IA] {
        def iterator = new Iterator[IA] {
          val leftIter = sortedLeft.iterator
          val rightIter = sortedRight.iterator

          var _left: IA = _
          var _right: IA = _

          def left() = {
            if (_left == null && leftIter.hasNext) {
              _left = leftIter.next()
            }
            _left
          }

          def right() = {
            if (_right == null && rightIter.hasNext) {
              _right = rightIter.next()
            }
            _right
          }

          def hasNext = left() != null || right() != null

          def next() = {
            val lf = left()
            val rt = right()

            if (lf == null) {
              val back = _right
              _right = nullIA
              back
            } else if (rt == null) {
              val back = _left
              _left = nullIA
              back
            } else {
              order.order(lf, rt) match {
                case LT => {
                  val back = _left
                  _left = nullIA
                  back
                }

                case GT => {
                  val back = _right
                  _right = nullIA
                  back
                }

                case EQ => {
                  val back = _left
                  while (order.order(back, left()) == EQ) {
                    _left = nullIA
                  }
                  while (order.order(back, right()) == EQ) {
                    _right = nullIA
                  }
                  back
                }
              }
            }
          }
        }
      }
    )
  }

  def intersect(d2: IterableDataset[A])(implicit ord: Order[A], ss: SortSerialization[IA]): IterableDataset[A] =  {
    val order = orderIA(ord)
    val sortedLeftIterable = iteratorSorting.sort(value.iterable.iterator, "union", IdGen.nextInt())
    val sortedRightIterable = iteratorSorting.sort(d2.iterable.iterator, "union", IdGen.nextInt())

    IterableDataset(
      value.idCount,
      new Iterable[IA] {
        def iterator = new Iterator[IA] {
          val sortedLeft = sortedLeftIterable.iterator
          val sortedRight = sortedRightIterable.iterator

          var _left = if (sortedLeft.hasNext) sortedLeft.next else nullIA
          var _right = if (sortedRight.hasNext) sortedRight.next else nullIA
          var _next : IA = precomputeNext

          def hasNext = _next != null

          def next = {
            val temp = _next

            _next = precomputeNext

            temp
          }

          @tailrec private def precomputeNext: IA = {
            if (_left == null || _right == null) {
               null
            } else {
              order.order(_left, _right) match {
                case EQ => 
                  val temp = _left
                  var stop = false

                  while (order.order(_left, _right) == EQ && !stop) {
                    if (!sortedLeft.hasNext) {
                      _left = nullIA
                      stop = true
                    }

                    _left = sortedLeft.next
                  }
                  stop = false
                  while (order.order(temp, _right) == EQ && !stop) {
                    if (!sortedRight.hasNext) {
                      _right = nullIA
                      stop = true
                    }
                    _right = sortedRight.next
                  }

                  temp

                case LT =>
                  while (order.order(_left, _right) == LT && sortedLeft.hasNext) {
                    _left = sortedLeft.next
                  }
                  precomputeNext

                case GT =>
                  while (order.order(_left, _right) == GT && sortedRight.hasNext) {
                    _right = sortedRight.next
                  }
                  precomputeNext
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

  def uniq(nextId: () => Identity, memoId: Int, memoCtx: MemoizationContext)(implicit order: Order[A], cm: ClassManifest[A], fs: SortSerialization[A]): IterableDataset[A] = {
    val filePrefix = "uniq"

    IterableDataset[A](1, 
      new Iterable[IA] {
        def iterator: Iterator[IA] = new Iterator[IA]{
          val sorted = iteratorSorting.sort(value.iterator, filePrefix, memoId).iterator
          private var atStart = true
          private var _next: A = precomputeNext()

          @tailrec private def precomputeNext(): A = {
            if (sorted.hasNext) {
              if (atStart) {
                atStart = false
                sorted.next
              } else {
                val tmp = sorted.next
                if (order.order(_next, tmp) == EQ) precomputeNext() else tmp
              }
            } else {
              null.asInstanceOf[A]
            }
          }

          def hasNext = _next != null
          def next = {
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

  def sortByIndexedIds(indices: Vector[Int], memoId: Int)(implicit cm: Manifest[A], fs: SortSerialization[IA]): IterableDataset[A] = IterableDataset(
    value.idCount, 
    iteratorSorting.sort(value.iterable.iterator, "sort-ids", memoId)
  )

  def memoize(memoId: Int): IterableDataset[A] = {
    val cache = value.iterable.toSeq
    IterableDataset(value.idCount, cache)
  }

  def group[K](memoId: Int)(keyFor: A => IterableDataset[K])(implicit ord: Order[K], kvs: SortSerialization[(K, Identities, A)]): IterableGrouping[K, IterableDataset[A]] = {
    implicit val kaOrder: Order[(K,Identities,A)] = new Order[(K,Identities,A)] {
      def order(x: (K,Identities,A), y: (K,Identities,A)) = {
        val kOrder = ord.order(x._1, y._1)
        if (kOrder == Ordering.EQ) {
          IdentitiesOrder.order(x._2, y._2)
        } else kOrder
      }
    }

    val withK = value.iterable.iterator.flatMap { case (i, a) => keyFor(a).iterator.map(k => (k, i, a)) }
    val sorted: Iterator[(K,Identities,A)] = iteratorSorting.sort(withK, "group", memoId).iterator

    IterableGrouping(new Iterator[(K, IterableDataset[A])] {
        // Have to hold our "peek" over the entire outer iterator
        var _next: (K, Identities, A) = sorted.next
        var sameK = true

        def hasNext = sorted.hasNext
        def next: (K, IterableDataset[A]) = {  
          val currentK = _next._1
          sameK = true
          
          // Compute an IterableDataset that will run over sorted for all == K
          val innerDS = IterableDataset[A](
            value.idCount, 
            new Iterable[(Identities,A)] {
              def iterator = new Iterator[(Identities,A)] {
                def hasNext = sameK
                def next = {
                  val tmp = _next
                  _next = sorted.next
                  sameK = ord.order(currentK, _next._1) == EQ
                  (tmp._2, tmp._3)
                }
              }
            }
          )

          val kChunkId = IdGen.nextInt()

          (currentK, extend(innerDS).memoize(kChunkId))
        }
      })
  }

  def perform(io: IO[_]): IterableDataset[A] = sys.error("todo")
}

// vim: set ts=4 sw=4 et:
