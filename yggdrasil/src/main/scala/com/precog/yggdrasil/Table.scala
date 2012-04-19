package com.precog.yggdrasil

import blueeyes.json.JsonAST._
import java.lang.ref.SoftReference
import scala.annotation.tailrec
import scalaz.Ordering._

trait Table { source =>
  import Table._

  def rowView: RowView

  def map(meta: CMeta, refId: Long)(f: F1P[_, _]): Table = new Table {
    def rowView = new MapRowView(source.rowView, meta, refId, f)
  }

  def cogroup(other: Table)(f: CogroupF): Table = {
    new Table {
      def rowView = new CogroupRowView(source.rowView, other.rowView, f)
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
                case (jv, meta @ CMeta(CPaths(_, selector), ctype)) => 
                  jv.set(selector, ctype.jvalueFor(ctype.cast(view.valueAt(meta))))

                case (jv, _) => jv
              }
          }
        }
      }
    }
  }
}

object Table {
  trait CogroupF {
    def one:  CMeta => Option[F1P[_, _]]
    def both: CMeta => Option[F2P[_, _, _]]
  }
}

class MapRowView(delegate: RowView, mapMeta: CMeta, refId: Long, f: F1P[_, _]) extends RowView { 
  class Position(private[MapRowView] val pos: delegate.Position)

  private val resultMeta = CMeta(CDyn(refId), f.returns)

  def state = delegate.state
  def advance() = delegate.advance()
  def position = new Position(delegate.position)
  def reset(position: Position) = delegate.reset(position.pos)

  protected[yggdrasil] def idCount: Int = delegate.idCount
  protected[yggdrasil] def columns: Set[CMeta] = delegate.columns + resultMeta

  protected[yggdrasil] def idAt(i: Int): Identity = delegate.idAt(i)
  protected[yggdrasil] def hasValue(meta: CMeta): Boolean = columns.contains(meta)
  protected[yggdrasil] def valueAt(meta: CMeta): Any = {
    if (meta == resultMeta) { 
      f.applyCast(delegate.valueAt(mapMeta))
    } else {
      delegate.valueAt(meta)
    }
  }
}

class CogroupRowView(left: RowView, right: RowView, f: Table.CogroupF) extends RowView { 
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

  protected[yggdrasil] def columns: Set[CMeta] = {
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

  protected[yggdrasil] def hasValue(cmeta: CMeta): Boolean = {
    fchoice match {
      case -1 => left.hasValue(cmeta)
      case  0 => left.hasValue(cmeta) || right.hasValue(cmeta)
      case  1 => right.hasValue(cmeta)
    }
  }

  protected[yggdrasil] def valueAt(cmeta: CMeta): Any = {
    val ctype = cmeta.ctype
    def applyF1(view: RowView, side: String): Any = {
      if (view.hasValue(cmeta)) {
        f.one(cmeta) map { f1 => f1.applyCast(view.valueAt(cmeta)) } getOrElse { view.valueAt(cmeta) }
      } else {
        sys.error("Column " + cmeta + " does not exist in " + side + " of cogroup at " + view.position)
      }
    }

    def applyF2: Any = {
      f.both(cmeta) map { f2 => 
        f2.applyCast(left.valueAt(cmeta), right.valueAt(cmeta))
      } getOrElse {
        sys.error("Could not determine function to combine column values from both the lhs and rhs of cogroup at " + left.position + ", " + right.position)
      }
    }

    fchoice match {
      case -1 => applyF1(left, "lhs")

      case  0 => 
        if (left.hasValue(cmeta) && right.hasValue(cmeta)) applyF2
        else if (left.hasValue(cmeta)) applyF1(left, "lhs")
        else if (right.hasValue(cmeta)) applyF1(right, "rhs")
        else sys.error("Column " + cmeta + " does not exist.")

      case  1 => applyF1(right, "rhs")
    }
  }
}

class SliceTable(slices: Iterable[Slice]) extends Table {
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
    protected[yggdrasil] def columns: Set[CMeta] = currentSlice.columns.keySet

    protected[yggdrasil] def idAt(i: Int): Identity = currentSlice.identities(i).apply(curIdx)

    protected[yggdrasil] def hasValue(meta: CMeta): Boolean = currentSlice.columns.contains(meta)

    protected[yggdrasil] def valueAt(meta: CMeta): Any = currentSlice.columns(meta).apply(curIdx)
  }

  override def map(meta: CMeta, refId: Long)(f: F1P[_, _]): Table = new SliceTable(slices map { slice => slice.map(meta, refId)(f.toF1) })
}
// vim: set ts=4 sw=4 et:
