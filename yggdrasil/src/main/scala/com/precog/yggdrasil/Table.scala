package com.precog.yggdrasil

import scalaz.Ordering._


trait Table { source =>
  import Table._

  def idCount: Int

  type RV <: RowView
  def rowView: RowView

  def cogroup(other: Table)(f: CogroupF): Table = {
    assert(source.idCount == other.idCount)

    new Table {
      val idCount = source.idCount

      def rowView = new CogroupRowView(source.rowView, other.rowView)
      class CogroupRowView(private val left: RowView, private val right: RowView) extends RowView { rv =>
        assert((left.state eq RowView.BeforeStart) && (right.state eq RowView.BeforeStart))

        class Position(private[CogroupRowView] val lpos: left.Position, private[CogroupRowView] val rpos: right.Position)

        private sealed trait CogroupState
        private case object Step extends CogroupState
        private case object LastEqual extends CogroupState
        private case object EndLeft extends CogroupState
        private case object RunLeft extends CogroupState
        private case class Cartesian(start: right.Position) extends CogroupState

        private var lastLeft: left.Position = null.asInstanceOf[left.Position]
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
              if (lastLeft == null) {
                leftState = left.advance()
                if (leftState ne RowView.AfterEnd) lastLeft = left.position
              }

              if (lastRight == null) {
                rightState = right.advance()
                if (rightState ne RowView.AfterEnd) lastRight = right.position
              }

              if ((leftState eq RowView.AfterEnd) && (rightState eq RowView.AfterEnd)) {
                setState(RowView.AfterEnd)
              } else if (rightState eq RowView.AfterEnd) {
                fchoice = -1
                lastLeft = null.asInstanceOf[left.Position] // we will never need to reposition the left
                setState(leftState)
              } else if (leftState eq RowView.AfterEnd) {
                fchoice = 1
                lastRight = null.asInstanceOf[right.Position] // we will never need to reposition the right
                setState(rightState)
              } else {
                left.compareIdentityPrefix(right, idCount) match {
                  case LT => lastLeft  = null.asInstanceOf[left.Position] // the right is greater, so the left will never need to return to its current position
                  case GT => lastRight = null.asInstanceOf[right.Position] // the left is greater, so the right will never need to return to its current position
                  case EQ => 
                    lastLeft  = left.position
                    lastRight = right.position
                    cs = LastEqual
                    fchoice = 0
                }

                setState(RowView.Data)
              }

            case LastEqual =>
              rightState = right.advance 
              rightState match {
                case RowView.AfterEnd =>
                  // The right side is out of elements, so, we reposition the right side to the previous
                  // location (which must be the last element on the right, since the only way to enter the LastEqual
                  // state is from a Step) and enter the RunLeft state, then advance to advance the left. The next element
                  // on the left must therefore be greater than or equal to the final position on the right.
                  cs = EndLeft
                  right.reset(lastRight)
                  setState(advance())

                case rs => 
                  left.compareIdentityPrefix(right, idCount) match {
                    case LT =>
                      right.reset(lastRight)
                      cs = RunLeft //before we exit the RunLeft state, we need to re-advance the right
                      setState(advance())

                    case GT => 
                      sys.error("Inputs on the right-hand side not sorted")

                    case EQ =>
                      // We have a run of at least two on the right, since the left was not advanced.
                      // lastLeft contains the start of the cartesian product on the lef hand side.
                      // in the Cartesian state, we will repeatedly advance the right until it no
                      // longer equals the left.
                      cs = Cartesian(lastRight)
                      fchoice = 0
                      _state
                  }
              }

            case EndLeft =>
              // run on the left side until it is no longer equal to the right position, then be done
              fchoice = -1
              setState(left.advance())

            case RunLeft =>
              // the next right element is bigger
              leftState = left.advance 
              leftState match {
                case RowView.AfterEnd => 
                  lastLeft = null.asInstanceOf[left.Position]
                  lastRight = null.asInstanceOf[right.Position] // this will cause the right to advance
                  cs = Step
                  setState(advance())

                case _ =>
                  left.compareIdentityPrefix(right, idCount) match {
                    case LT => sys.error("Inputs on the left-hand side not sorted")

                    case GT =>
                      // left has jumped past right, so we're off the run; advance the right and then 
                      // start from square one
                      rightState = right.advance()
                      lastRight = right.position
                      cs = Step
                      setState(advance())

                    case EQ =>
                      // stay in the RunLeft state, since we know that the next right element is bigger
                      fchoice = 0
                      _state
                  }
              }

              case Cartesian(start) =>
                rightState = right.advance()
                left.compareIdentityPrefix(right, idCount) match {
                  case LT => 
                    right.reset(start)
                    leftState = left.advance()
                    left.compareIdentityPrefix(right, idCount) match {
                      case LT => 
                        sys.error("Inputs on the left-hand side not sorted")

                      case EQ => 
                        //do nothing; fchoice is already 0 and we're still equal
                        fchoice = 0
                        _state

                      case GT => 
                        //jump past the end of the run on the right
                        right.reset(lastRight)
                        cs = Step
                        setState(advance())
                    }

                  case GT => 
                    sys.error("Inputs on the right-hand side not sorted")
                    
                  case EQ => 
                    //do nothing; fchoice is already 0 and we're still equal
                    fchoice = 0
                    _state
                }
          }
        }

        def reset(position: Position): RowView.State = {
          left.reset(position.lpos)
          right.reset(position.rpos)
        }
        
        protected[yggdrasil] def idCount: Int = source.idCount

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
              f.one.get(cmeta) map { f1 => f1.applyCast(view.valueAt(cmeta)) } getOrElse { view.valueAt(cmeta) }
            } else {
              sys.error("Column " + cmeta + " does not exist in " + side + " of cogroup at " + view.position)
            }
          }

          def applyF2: Any = {
            f.both.get(cmeta) map { f2 => 
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
    }
  }
}

object Table {
  trait CogroupF {
    def one:  Map[CMeta, CF1[_, _]]
    def both: Map[CMeta, CF2[_, _, _]]
  }
}

// vim: set ts=4 sw=4 et:
