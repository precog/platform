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

import blueeyes.json.JsonAST._
import java.lang.ref.SoftReference

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec
import scalaz.Ordering._

/*
trait Table { source =>
  val foci: Set[VColumnRef]

  def rowView: RowView

  def map(f: F1P[_, _])(implicit nextRef: () => Long): Table = {
    foci.filter(_.ctype == f.accepts).foldLeft(this) {
      case (t, cmeta) if cmeta.ctype == f.accepts => t.map(cmeta.id, nextRef)(f)
    }
  }

  def map(colId: VColumnId, nextRef: () => Long)(f: F1P[_, _]): Table = new Table {
    private val mappings = source.foci collect { 
      case ref @ VColumnRef(id, ctype) if id == colId && ctype == f.accepts => (VColumnRef(DynColumnId(nextRef()), f.returns): VColumnRef, ref)
    } toMap

    val foci = mappings.keySet

    def rowView = new MapRowView(source.rowView, mappings, f)
  }

  def cogroup(other: Table)(merge: CogroupMerge): Table = {
    new Table {
      val foci = source.foci ++ other.foci
      def rowView: RowView = new CogroupRowView(source.rowView, other.rowView, merge)
    }
  }

  def toJson: Iterable[JValue] = {
    new Iterable[JValue] {
      def iterator = new Iterator[JValue] {
        private val view = source.rowView
        private var _next: JValue = precomputeNext()

        def hasNext = _next != null

        def next = {
          val tmp = _next
          _next = precomputeNext
          tmp
        }

        private def precomputeNext(): JValue = {
          view.advance() match {
            case RowView.AfterEnd => null.asInstanceOf[JValue]
            case _ =>
              view.columns.foldLeft[JValue](JNull) {
                case (jv, ref @ VColumnRef(NamedColumnId(_, selector), ctype)) => 
                  jv.set(selector, ctype.jvalueFor(view.valueAt[ctype.CA](VColumnRef.cast[ctype.CA](ref))))

                case (jv, _) => jv
              }
          }
        }
      }
    }
  }
}

class MapRowView(delegate: RowView, mappings: Map[VColumnRef, VColumnRef], f: F1P[_, _]) extends RowView { 
  class Position(private[MapRowView] val pos: delegate.Position)

  def state = delegate.state
  def advance() = delegate.advance()
  def position = new Position(delegate.position)
  def reset(position: Position) = delegate.reset(position.pos)

  protected[yggdrasil] val idCount: Int = delegate.idCount
  protected[yggdrasil] val columns: Set[VColumnRef] = delegate.columns ++ mappings.keySet

  protected[yggdrasil] def idAt(i: Int): Identity = delegate.idAt(i)
  protected[yggdrasil] def hasValue(meta: VColumnRef): Boolean = columns.contains(meta)
  protected[yggdrasil] def valueAt[@specialized(Boolean, Int, Long, Float, Double) A](rref: VColumnRef { type CA = A }): A = {
    import VColumnRef.cast
    mappings.get(rref) map {
      case aref @ VColumnRef(id, CBoolean) => f.asInstanceOf[F1P[Boolean, A]](delegate.valueAt(cast[Boolean](aref)))
      case aref @ VColumnRef(id, CInt)     => f.asInstanceOf[F1P[Int, A]](delegate.valueAt(cast[Int](aref)))
      case aref @ VColumnRef(id, CLong)    => f.asInstanceOf[F1P[Long, A]](delegate.valueAt(cast[Long](aref)))
      case aref @ VColumnRef(id, CFloat)   => f.asInstanceOf[F1P[Float, A]](delegate.valueAt(cast[Float](aref)))
      case aref @ VColumnRef(id, CDouble)  => f.asInstanceOf[F1P[Double, A]](delegate.valueAt(cast[Double](aref)))
      case aref @ VColumnRef(id, argt)     => f.asInstanceOf[F1P[argt.CA, A]](delegate.valueAt(cast[argt.CA](aref)))
    } getOrElse {
      delegate.valueAt[rref.CA](rref)
    }
  }
}
*/

trait CogroupMerge {
  def apply[A](ctype: VColumnRef { type CA = A }): Option[F2P[A, A, A]]
}

/*
class CogroupRowView(left: RowView, right: RowView, merge: CogroupMerge) extends RowView { 
  assert((left.state eq RowView.BeforeStart) && (right.state eq RowView.BeforeStart))

  class Position(private[CogroupRowView] val lpos: left.Position, private[CogroupRowView] val rpos: right.Position)

  private sealed trait CogroupState
  private case object Step extends CogroupState
  private case object LastEqual extends CogroupState
  private case object EndLeft extends CogroupState
  private case object RunLeft extends CogroupState
  private case class Cartesian(start: right.Position) extends CogroupState

  private var stepLeft: Boolean = true
  private var leftState = left.state

  private var lastRight: right.Position = null.asInstanceOf[right.Position]
  private var rightState = right.state

  private var cs: CogroupState = Step
  private var fchoice: Int = 0
  private var _state: RowView.State = RowView.BeforeStart

  def position = new Position(left.position, right.position)

  def state = _state
  @inline private def setState(s: RowView.State): RowView.State = {
    _state = s
    _state
  }

  def advance(): RowView.State = if (_state eq RowView.AfterEnd) _state else {
    cs match {
      case Step => 
        if (stepLeft) {
          leftState = left.advance()
          stepLeft = (left.state eq RowView.Data)
        }

        if (lastRight == null) {
          rightState = right.advance()
          lastRight = rightState match {
            case  RowView.Data => right.position
            case _ => null.asInstanceOf[right.Position]
          }
        }

        if ((leftState eq RowView.AfterEnd) && (rightState eq RowView.AfterEnd)) {
          setState(RowView.AfterEnd)
        } else if (rightState eq RowView.AfterEnd) {
          fchoice = -1
          setState(leftState)
        } else if (leftState eq RowView.AfterEnd) {
          fchoice = 1
          lastRight = null.asInstanceOf[right.Position] //flag the right to advance
          setState(rightState)
        } else {
          left.compareIdentityPrefix(right, idCount) match {
            case LT => 
              // the right is greater, flag the left to advance to catch it up
              // and the right will remain at its current position since lastRight has not
              // been modified
              stepLeft = true
              fchoice = -1

            case GT => 
              // the left is greater, so flag the right to advance, and the left to not do so,
              // until the right catches up or passes
              stepLeft  = false
              lastRight = null.asInstanceOf[right.Position] 
              fchoice = 1

            case EQ => 
              stepLeft  = false // don't advance the left until the right hand side is ahead
              lastRight = right.position
              cs = LastEqual
              fchoice = 0
          }

          setState(RowView.Data)
        }

      case LastEqual =>
        rightState = right.advance()
        rightState match {
          case RowView.AfterEnd =>
            // The right side is out of elements, so, we reposition the right side to the previous
            // location (which must be the last element on the right, since the only way to enter the LastEqual
            // state is from a Step) and enter the RunLeft state, then advance to advance the left. The next element
            // on the left must therefore be greater than or equal to the final position on the right.
            cs = EndLeft
            right.reset(lastRight)
            setState(advance())

          case _ => 
            left.compareIdentityPrefix(right, idCount) match {
              case LT =>
                right.reset(lastRight)
                cs = RunLeft //before we exit the RunLeft state, we need to re-advance the right
                setState(advance())

              case GT => 
                sys.error("Inputs on the right-hand side not sorted")

              case EQ =>
                // We have a run of at least two on the right, since the left was not advanced.
                // in the Cartesian state, we will repeatedly advance the right until it no
                // longer equals the left.
                cs = Cartesian(lastRight)
                _state
            }
        }

      case EndLeft =>
        // run on the left side until it is no longer equal to the right position, then be done
        leftState = left.advance()
        leftState match {
          case RowView.AfterEnd => 
            setState(RowView.AfterEnd)

          case _ =>
            left.compareIdentityPrefix(right, idCount) match {
              case LT => sys.error("Inputs on the left-hand side not sorted")

              case GT =>
                // left has surpassed the right; since the right hand side is at the end,
                // just take the left element.
                fchoice = -1
                setState(leftState)

              case EQ =>
                fchoice = 0
                setState(leftState)
            }
        }

      case RunLeft =>
        // the next right element is bigger than the current left at this point
        leftState = left.advance 
        leftState match {
          case RowView.AfterEnd => 
            stepLeft = false // no need to keep advancing the left
            lastRight = null.asInstanceOf[right.Position] // this will cause the right to advance
            cs = Step
            setState(advance())

          case _ =>
            left.compareIdentityPrefix(right, idCount) match {
              case LT => sys.error("Inputs on the left-hand side not sorted")

              case GT =>
                // left has jumped past right, so we're off the run; so flag the right to advance and then 
                // return to the step state
                stepLeft = false
                lastRight = null.asInstanceOf[right.Position]
                cs = Step
                setState(advance())

              case EQ =>
                // stay in the RunLeft state, since we know that the next right element is bigger
                fchoice = 0
                _state
            }
        }

        case Cartesian(start) =>
          // fchoice is always 0 when entering the cartesian state
          
          def advanceLeft = {
            leftState = left.advance()
            leftState match {
              case RowView.AfterEnd =>
                // cartesian is done, right has been advanced, so we ensure that the right doesn't
                // skip and return to the step state
                stepLeft = false
                lastRight = right.position
                cs = Step
                setState(advance())

              case _ => 
                // save the position on the right, then reset the right to the start of the run
                // so that we can compare the advanced left
                lastRight = right.position
                right.reset(start)
                left.compareIdentityPrefix(right, idCount) match {
                  case LT => 
                    sys.error("Inputs on the left-hand side not sorted")

                  case EQ => 
                    //do nothing continuing to re-advance the right over the entire run
                    _state

                  case GT => 
                    // left has passed the end of the run on the right, so move the right forward
                    // to the first element beyond the end of the run
                    right.reset(lastRight)
                    cs = Step
                    setState(advance())
                }
            }
          }

          rightState = right.advance()
          rightState match {
            case RowView.AfterEnd => 
              advanceLeft

            case _ => 
              left.compareIdentityPrefix(right, idCount) match {
                case LT => 
                  advanceLeft

                case GT => sys.error("Inputs on the right-hand side not sorted")
                  
                case EQ => 
                  // do nothing, since we're still in the cartesian product advancing the right hand side
                  _state
              }
          }
    }
  }

  def reset(position: Position): RowView.State = {
    left.reset(position.lpos)
    right.reset(position.rpos)
  }
  
  protected[yggdrasil] def idCount: Int = left.idCount min right.idCount

  protected[yggdrasil] def columns: Set[VColumnRef] = {
    fchoice match {
      case -1 => left.columns
      case  0 => left.columns ++ right.columns
      case  1 => right.columns
    }
  }

  protected[yggdrasil] def idAt(idx: Int) = fchoice match {
    case -1 | 0 => left.idAt(idx)
    case 1 => right.idAt(idx)
  }

  protected[yggdrasil] def hasValue(cmeta: VColumnRef): Boolean = {
    fchoice match {
      case -1 => left.hasValue(cmeta)
      case  0 => left.hasValue(cmeta) || right.hasValue(cmeta)
      case  1 => right.hasValue(cmeta)
    }
  }

  protected[yggdrasil] def valueAt[@specialized(Boolean, Int, Long, Float, Double) A](ref: VColumnRef { type CA = A }): A = {
    def value(view: RowView, side: String): A = {
      if (view.hasValue(ref)) {
        view.valueAt[ref.CA](ref)
      } else {
        sys.error("Column " + ref + " does not exist in " + side + " of cogroup at " + view.position)
      }
    }

    def merged: A = {
      merge(ref) map { f2 => 
        val t_1 = f2.accepts._1
        val t_2 = f2.accepts._2
        f2.asInstanceOf[F2P[A, A, A]](left.valueAt[A](ref), right.valueAt[A](ref))
      } getOrElse {
        sys.error("Could not determine function to combine column values from both the lhs and rhs of cogroup at " + left.position + ", " + right.position)
      }
    }

    fchoice match {
      case -1 => value(left, "lhs")

      case  0 => 
        if (left.hasValue(ref) && right.hasValue(ref)) merged
        else if (left.hasValue(ref)) value(left, "lhs")
        else if (right.hasValue(ref)) value(right, "rhs")
        else sys.error("Column " + ref + " does not exist.")

      case  1 => value(right, "rhs")
    }
  }
}
*/

class Table(val idCount: Int, val foci: Set[VColumnRef], val slices: Iterable[Slice]) { self  =>
  /*
  def rowView: RowView = new RowView {
    private val iter = slices.iterator
    private var currentSlice: Slice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
    private var curIdx: Int = -1
    private var allocatedPositions: List[SoftReference[Position]] = Nil

    private var resetPosition: Position = null.asInstanceOf[Position]
    private var resetIndex: Int = 0

    class Position(private[SliceTable] val idx: Int, slice: Slice) {
      // a vector of hard references to 
      private var slices: Vector[Slice] = Vector(slice)
      private[SliceTable] def append(slice: Slice): Position = {
        slices :+= slice
        this
      }

      private[SliceTable] def size: Int = slices.length
      private[SliceTable] def sliceAt(resetIndex: Int) = slices(resetIndex)
    }

    def position: Position = {
      val position = new Position(curIdx, currentSlice)
      allocatedPositions = new SoftReference(position) :: allocatedPositions
      position
    }

    def state = if (currentSlice == null) RowView.AfterEnd
                else if (curIdx > -1) RowView.Data
                else RowView.BeforeStart

    @tailrec def advance(): RowView.State = {
      if (resetPosition ne null) {
        // since we've been reset, we'll use the tail of the reset position until it is exhausted before
        // advancing the underlying iterator.
        curIdx += 1
        if (curIdx > currentSlice.size) {
          curIdx = -1
          resetIndex += 1
          if (resetIndex < resetPosition.size) {
            currentSlice = resetPosition.sliceAt(resetIndex)
          } else {
            // we've exhausted the slices of the reset position, so go ahead and advance the 
            resetIndex = 0
            resetPosition = null
            currentSlice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
          }

          advance()
        } else {
          RowView.Data
        }
      } else if (currentSlice eq null) {
        RowView.AfterEnd 
      } else {
        // advance over the slice iterator as normal; whenever we move to a new slice,
        // update any allocated poitions that haven't been freed to record references
        // to the slices in the tail of the positions, so that they don't get GCed
        // before references to them may be needed
        curIdx += 1
        if (curIdx > currentSlice.size) {
          curIdx = -1
          currentSlice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
          if (currentSlice ne null) {
            allocatedPositions = allocatedPositions filter { pref => 
              val pos = pref.get
              // yay for evil, side effects in a filter block!
              if (pos != null) pos.append(currentSlice)
              pos == null
            }
          }

          advance()
        } else {
          RowView.Data
        }
      }
    }

    def reset(position: Position): RowView.State = {
      curIdx = position.idx
      resetPosition = position
      if (curIdx == -1) RowView.BeforeStart else RowView.Data
    }

    protected[yggdrasil] def idCount: Int = currentSlice.idCount
    protected[yggdrasil] def columns: Set[VColumnRef] = currentSlice.columns.keySet

    protected[yggdrasil] def idAt(i: Int): Identity = currentSlice.identities(i).apply(curIdx)

    protected[yggdrasil] def hasValue(ref: VColumnRef): Boolean = currentSlice.columns.contains(ref)

    protected[yggdrasil] def valueAt[@specialized(Boolean, Int, Long, Float, Double) A](ref: VColumnRef { type CA = A }): A = currentSlice.columns(ref)(curIdx).asInstanceOf[A]
  }
  */

  def map(colId: VColumnId, nextRef: () => Long)(f: F1P[_, _]): Table = {
    val oldRef = VColumnRef(colId, f.accepts)
    val newId  = DynColumnId(nextRef())
    val newRef = VColumnRef(newId, f.returns)
    new Table(idCount, foci - oldRef + newRef, slices map { slice => slice.map(colId, newId)(f.toF1) })
  }

  def cogroup(other: Table)(merge: CogroupMerge): Table = {
    sealed trait CogroupState
    case object StepLeftCheckRight extends CogroupState
    case object StepLeftDoneRight extends CogroupState
    case object StepRightCheckLeft extends CogroupState
    case object StepRightDoneLeft extends CogroupState
    case object LastEqual extends CogroupState
    case object RunLeft extends CogroupState
    case object Done extends CogroupState
    case object Cartesian extends CogroupState

    new Table(
      idCount,
      foci ++ other.foci,
      new Iterable[Slice] {
        def iterator = new Iterator[Slice] {
          private val prefixLength = self.idCount min other.idCount
          private val leftIter = self.slices.iterator
          private val rightIter = other.slices.iterator
          
          private var leftSlice = if (leftIter.hasNext) leftIter.next else null.asInstanceOf[Slice]
          private var rightSlice = if (rightIter.hasNext) rightIter.next else null.asInstanceOf[Slice]

          private var leftIdx = 0 
          private var rightIdx = 0
          private var firstRightEq: Int = -1
          private var nextRight: Int = -1

          private var leftBuffer: ArrayBuffer[Int] = new ArrayBuffer[Int]()
          private var rightBuffer: ArrayBuffer[Int] = new ArrayBuffer[Int]()

          def bufferRemap(buf: ArrayBuffer[Int]): PartialFunction[Int, Int] = {
            case i if (i < buf.size) && buf(i) != -1 => buf(i)
          }

          private var state: CogroupState =
            if (leftSlice == null) {
              if (rightSlice == null) Done else StepRightDoneLeft
            } else {
              if (rightSlice == null) StepLeftDoneRight else StepLeftCheckRight
            }

          private var curSlice = precomputeNext()

          def hasNext: Boolean = curSlice != null
          
          def next: Slice = {
            val tmp = curSlice
            curSlice = precomputeNext()
            tmp
          }

          @tailrec private def precomputeNext(): Slice = {
            state match {
              case StepLeftCheckRight => 
                if (leftIdx < leftSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      leftBuffer += leftIdx
                      rightBuffer += -1

                      leftIdx += 1

                    case GT =>
                      state = StepRightCheckLeft

                    case EQ =>
                      leftBuffer += leftIdx
                      rightBuffer += rightIdx
                      state = LastEqual
                  }

                  precomputeNext()
                } else {
                  val result = emit()
                  if (leftIter.hasNext) {
                    leftSlice = leftIter.next 
                    leftIdx = 0
                  } else {
                    state = StepRightDoneLeft
                  }
                  result
                }

              case LastEqual =>
                firstRightEq = rightIdx
                rightIdx += 1

                if (rightIdx < rightSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      leftIdx += 1
                      state = StepLeftCheckRight

                    case GT =>
                      sys.error("Inputs on the right not sorted.")
                      
                    case EQ => 
                      leftBuffer += leftIdx
                      rightBuffer += rightIdx
                      rightIdx += 1
                      state = Cartesian
                  }
                } else {
                  if (rightIter.hasNext) {
                    rightSlice = (rightSlice append rightIter.next)
                  } else {
                    state = StepLeftDoneRight
                    leftIdx += 1
                  }
                }

                precomputeNext()

              case Cartesian =>
                leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                  case LT => 
                    nextRight = rightIdx
                    rightIdx = firstRightEq
                    leftIdx += 1
                    
                    if (leftIdx >= leftSlice.size && leftIter.hasNext) {
                      leftSlice = (leftSlice append leftIter.next)
                    
                      leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                        case LT => sys.error("Inputs on the left not sorted")
                        case GT => 
                          rightIdx = nextRight
                          state = StepLeftCheckRight
                          if (leftBuffer.size > leftSlice.size) emit() else precomputeNext()
                        
                        case EQ => 
                          leftBuffer += leftIdx
                          rightBuffer += rightIdx
                          rightIdx += 1
                          precomputeNext()
                      }
                    } else {
                      state = StepRightDoneLeft
                      if (leftBuffer.size > leftSlice.size) emit() else precomputeNext()
                    }

                  case GT =>
                    sys.error("Inputs on the right not sorted.")
                    
                  case EQ => 
                    leftBuffer += leftIdx
                    rightBuffer += rightIdx
                    rightIdx += 1

                    if (rightIdx >= rightSlice.size && rightIter.hasNext) {
                      rightSlice = rightSlice append rightIter.next
                    }

                    precomputeNext()
                }
              }
          }

          private def emit(): Slice = {
            val result = new Slice {
              private val remappedLeft  = leftSlice.remap(bufferRemap(leftBuffer))
              private val remappedRight = rightSlice.remap(bufferRemap(rightBuffer))

              val idCount = self.idCount + other.idCount
              val size = leftBuffer.size
              val identities = remappedLeft.identities ++ remappedRight.identities

              val columns = remappedRight.columns.foldLeft(remappedLeft.columns) {
                case (acc, (rref, rcol)) => 
                  val mergef = merge[rref.CA](rref).get
                  acc.get(rref) match {
                    case Some(lcol) => acc + (rref -> (rref.ctype match {
                      case CBoolean => new Column[Boolean] {
                        private val lc = lcol.asInstanceOf[Column[Boolean]]
                        private val rc = rcol.asInstanceOf[Column[Boolean]]
                        private val mf = mergef.asInstanceOf[F2P[Boolean, Boolean, Boolean]]

                        val returns = CBoolean
                        
                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)
                        
                        def apply(row: Int): Boolean = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }

                      case CLong    => new Column[Long] {
                        private val lc = lcol.asInstanceOf[Column[Long]]
                        private val rc = rcol.asInstanceOf[Column[Long]]
                        private val mf = mergef.asInstanceOf[F2P[Long, Long, Long]]

                        val returns = CLong

                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)

                        def apply(row: Int): Long = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }

                      case CDouble  => new Column[Double] {
                        private val lc = lcol.asInstanceOf[Column[Double]]
                        private val rc = rcol.asInstanceOf[Column[Double]]
                        private val mf = mergef.asInstanceOf[F2P[Double, Double, Double]]

                        val returns = CDouble

                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)

                        def apply(row: Int): Double = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }

                      case ctype    => new Column[ctype.CA] {
                        private val lc = lcol.asInstanceOf[Column[ctype.CA]]
                        private val rc = rcol.asInstanceOf[Column[ctype.CA]]
                        private val mf = mergef.asInstanceOf[F2P[ctype.CA, ctype.CA, ctype.CA]]

                        val returns: CType { type CA = ctype.CA } = ctype

                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)

                        def apply(row: Int): ctype.CA = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }
                    }))
                  }
              }
            }

            leftSlice = leftSlice.split(leftIdx)._2
            rightSlice = rightSlice.split(rightIdx)._2
            leftIdx = 0
            rightIdx = 0
            leftBuffer = new ArrayBuffer[Int]()
            rightBuffer = new ArrayBuffer[Int]()
            result
          }
        }
      }
    )
  }
}
// vim: set ts=4 sw=4 et:
