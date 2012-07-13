package com.precog.yggdrasil
package table

import com.precog.common.{Path, VectorCase}

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList
import org.joda.time.DateTime

import scala.collection.BitSet
import scala.collection.Set
import scala.annotation.tailrec

import scalaz._
import scalaz.Ordering._
import scalaz.std.function._
import scalaz.std.list._
import scalaz.std.tuple._
import scalaz.std.iterable._
import scalaz.syntax.arrow._
import scalaz.syntax.traverse._

trait ColumnarTableModule extends TableModule {
  import trans._
  import trans.constants._
  import Schema._

  type F1 = CF1
  type F2 = CF2
  type Scanner = CScanner
  type Reducer[α] = CReducer[α]
  type RowId = Int
  
  trait ColumnarTableOps extends TableOps {
    def loadStatic(path: Path): Table = sys.error("todo")
    def loadDynamic(source: Table): Table = sys.error("todo")
    
    def empty: Table = new Table(Iterable.empty[Slice])
    
    def constBoolean(v: Set[CBoolean]): Table = {
      val column = ArrayBoolColumn(v.toArray map(_.value))
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CBoolean) -> column), v.size)))
    }

    def constLong(v: Set[CLong]): Table = {
      val column = ArrayLongColumn(v.toArray map(_.value))
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CLong) -> column), v.size)))
    }

    def constDouble(v: Set[CDouble]): Table = {
      val column = ArrayDoubleColumn(v.toArray map(_.value))
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CDouble) -> column), v.size)))
    }

    def constDecimal(v: Set[CNum]): Table = {
      val column = ArrayNumColumn(v.toArray map(_.value))
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CDecimalArbitrary) -> column), v.size)))
    }

    def constString(v: Set[CString]): Table = {
      val column = ArrayStrColumn(v.toArray map(_.value))
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CStringArbitrary) -> column), 1)))
    }

    def constDate(v: DateTime): Table =  //TODO should take a set; use CDate
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CDate) -> Column.const(v)), 1)))

    def constNull: Table = 
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CDate) -> new InfiniteColumn with NullColumn), 1)))

    def constEmptyObject: Table = 
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CDate) -> new InfiniteColumn with EmptyObjectColumn), 1)))

    def constEmptyArray: Table = 
      new Table(List(Slice(Map(ColumnRef(JPath.Identity, CDate) -> new InfiniteColumn with EmptyArrayColumn), 1)))
  }

  def ops: TableOps = new ColumnarTableOps {} 

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = new CF1(f(Column.const(cv), _))
    def applyr(cv: CValue) = new CF1(f(_, Column.const(cv)))

    def andThen(f1: F1) = new CF2((c1, c2) => f(c1, c2) flatMap f1)
  }

  case class SliceTransform[A](initial: A, f: (A, Slice) => (A, Slice)) {
    def andThen[B](t: SliceTransform[B]): SliceTransform[(A, B)] = {
      SliceTransform(
        (initial, t.initial),
        { case ((a, b), s) => 
            val (a0, sa) = f(a, s) 
            val (b0, sb) = t.f(b, sa)
            ((a0, b0), sb)
        }
      )
    }

    def zip[B](t: SliceTransform[B])(combine: (Slice, Slice) => Slice): SliceTransform[(A, B)] = {
      SliceTransform(
        (initial, t.initial),
        { case ((a, b), s) =>
            val (a0, sa) = f(a, s)
            val (b0, sb) = t.f(b, s)
            assert(sa.size == sb.size)
            ((a0, b0), combine(sa, sb))
        }
      )
    }
  }

  object SliceTransform {
    def identity[A](initial: A) = SliceTransform(initial, (a: A, s: Slice) => (a, s))
  }

  class Table(val slices: Iterable[Slice]) extends TableLike { self  =>
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce[A: Monoid](reducer: Reducer[A]): A = {  //todo for optimization, instead of composing just in the case when we're doing multiple reductions over the same column, we can do reductions over multiple columns in a given table!
      slices flatMap { s => 
        //assert(s.columns.size <= 1)
        s.columns.values map { col => reducer.reduce(col, 0 until s.size) } 
      } suml
    }

    def compact(spec: TransSpec1): Table = sys.error("todo")

    private def map0(f: Slice => Slice): SliceTransform[Unit] = SliceTransform[Unit]((), Function.untupled(f.second[Unit]))

    private def transform0[A](sliceTransform: SliceTransform[A]): Table = {
      new Table(
        new Iterable[Slice] {
          def iterator: Iterator[Slice] = {
            val baseIter = slices.iterator
            new Iterator[Slice] {
              private var state: A = sliceTransform.initial
              private var next0: Slice = precomputeNext()

              private def precomputeNext(): Slice = {
                if (baseIter.hasNext) {
                  val s = baseIter.next
                  val (nextState, s0) = sliceTransform.f(state, s)
                  state = nextState
                  s0
                } else {
                  null.asInstanceOf[Slice]
                }
              }

              def hasNext: Boolean = next0 != null
              def next: Slice = {
                val tmp = next0
                next0  = precomputeNext()
                tmp
              }
            }
          }
        }
      )
    }

    // No transform defined herein may reduce the size of a slice. Be it known!
    private def composeSliceTransform(spec: TransSpec1): SliceTransform[_] = {
      spec match {
        case Leaf(_) => SliceTransform.identity[Unit](())

        case Map1(source, f) => 
          composeSliceTransform(source) andThen {
             map0 { _ mapColumns f }
          }

        case Map2(left, right, f) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = 
                (for {
                  cl <- sl.valueColumns
                  cr <- sr.valueColumns
                  col <- f(cl, cr) // TODO: Unify columns of the same result type
                } yield {
                  (ColumnRef(JPath.Identity, col.tpe), col)
                })(collection.breakOut)
            }
          }

        case Filter(source, predicate) => 
          composeSliceTransform(source).zip(composeSliceTransform(predicate)) { (s, filter) => 
            if (s.columns.isEmpty) {
              s
            } else {
              val definedAt = filter.columns.values.foldLeft(BitSet(0 until s.size: _*)) { (acc, col) =>
                cf.util.isSatisfied(col).map(_.definedAt(0, s.size) & acc).getOrElse(BitSet.empty) 
              }

              s mapColumns { cf.util.filter(0, s.size, definedAt) }
            }
          }

        case Equal(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = {
                // 'excluded' is the set of columns that do not exist on both sides of the equality comparison
                // if, for a given row, any of these columns' isDefinedAt returns true, then
                // the result is defined for that row, and its value is false. If isDefinedAt
                // returns false for all columns, then the result (true, false, or undefined) 
                // must be determined by comparing the remaining columns pairwise.

                // In the following fold, we compute all paired columns, and the columns on the left that
                // have no counterpart on the right.
                val (paired, excludedLeft) = sl.columns.foldLeft((Map.empty[JPath, Column], Set.empty[Column])) {
                  case ((paired, excluded), (ref @ ColumnRef(selector, CLong | CDouble | CDecimalArbitrary), col)) => 
                    val numEq = for {
                                  ctype <- CLong :: CDouble :: CDecimalArbitrary :: Nil
                                  col0  <- sr.columns.get(ColumnRef(selector, ctype)) 
                                  boolc <- cf.std.Eq(col, col0)
                                } yield boolc

                    if (numEq.isEmpty) {
                      (paired, excluded + col)
                    } else {
                      val resultCol = new BoolColumn {
                        def isDefinedAt(row: Int) = {
                          numEq exists { _.isDefinedAt(row) }
                        }
                        def apply(row: Int) = {
                          numEq exists { 
                            case col: BoolColumn => col.isDefinedAt(row) && col(row) 
                            case _ => sys.error("Unreachable code - only boolean columns can be derived from equality.")
                          }
                        }
                      }

                      (paired + (selector -> paired.get(selector).flatMap(cf.std.And(_, resultCol)).getOrElse(resultCol)), excluded)
                    }

                  case ((paired, excluded), (ref, col)) =>
                    sr.columns.get(ref) flatMap { col0 =>
                      cf.std.Eq(col, col0) map { boolc =>
                        // todo: This line contains something that might be an error case going to none, but I can't see through it
                        // well enough to know for sure. Please review.
                        (paired + (ref.selector -> paired.get(ref.selector).flatMap(cf.std.And(_, boolc)).getOrElse(boolc)), excluded)
                      }
                    } getOrElse {
                      (paired, excluded + col)
                    }
                }

                val excluded = excludedLeft ++ sr.columns.collect({
                  case (ColumnRef(selector, CLong | CDouble | CDecimalArbitrary), col) 
                    if !(CLong :: CDouble :: CDecimalArbitrary :: Nil).exists(ctype => sl.columns.contains(ColumnRef(selector, ctype))) => col

                  case (ref, col) if !sl.columns.contains(ref) => col
                })

                val allColumns = sl.columns ++ sr.columns
                
                val resultCol = new MemoBoolColumn(
                  new BoolColumn {
                    def isDefinedAt(row: Int): Boolean = {
                      allColumns exists { case (_, c) => c.isDefinedAt(row) } 
                    }

                    def apply(row: Int): Boolean = {
                      !(
                        // if any excluded column exists for the row, unequal
                        excluded.exists(_.isDefinedAt(row)) || 
                         // if any paired column compares unequal, unequal
                        paired.exists({ case (_, equal: BoolColumn) => equal.isDefinedAt(row) && !equal(row) })
                      )
                    }
                  }
                )
                
                Map(ColumnRef(JPath.Identity, CBoolean) -> resultCol)
              }
            }
          }

        case WrapObject(source, field) =>
          composeSliceTransform(source) andThen {
            map0 { _ wrap JPathField(field) }
          }

        case WrapArray(source) =>
          composeSliceTransform(source) andThen {
            map0 { _ wrap JPathIndex(0) }
          }

        case ObjectConcat(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns = 
                // left side first in the seq so that the right side wins
                (sl.columns.toSeq ++ sr.columns).foldLeft(Map.empty[ColumnRef, Column]) {
                  case (acc, (ref, col)) if ref.selector.head.exists(_.isInstanceOf[JPathField]) => acc + (ref -> col)
                  case (acc, _) => acc
                }
            }
          }

        case ArrayConcat(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            def assertDense(paths: Set[JPath]) = assert {
              (paths collect { case JPath(JPathIndex(i), _ @ _*) => i }).toList.sorted.zipWithIndex forall { case (a, b) => a == b }
            }

            assertDense(sl.columns.keySet.map(_.selector))
            assertDense(sr.columns.keySet.map(_.selector))

            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = {
                val (indices, lcols) = sl.columns.toList map { case t @ (ColumnRef(JPath(JPathIndex(i), xs @ _*), _), _) => (i, t) } unzip
                val maxIndex = indices.reduceLeftOption(_ max _).map(_ + 1).getOrElse(0)
                val rcols = sr.columns map { case (ColumnRef(JPath(JPathIndex(j), xs @ _*), ctype), col) => (ColumnRef(JPath(JPathIndex(j + maxIndex) +: xs : _*), ctype), col) }
                lcols.toMap ++ rcols
              }
            }
          }

        case Typed(source, tpe) =>
          composeSliceTransform(source) andThen {
            map0 { _ typed tpe }
          }

        case Scan(source, scanner) => 
          composeSliceTransform(source) andThen {
            SliceTransform[scanner.A](
              scanner.init,
              (state: scanner.A, slice: Slice) => {
                assert(slice.columns.size <= 1)
                slice.columns.headOption flatMap {
                  case (ColumnRef(selector, ctype), col) =>
                    val (nextState, nextCol) = scanner.scan(state, col, 0 until slice.size)
                    nextCol map { c =>
                      ( nextState, 
                        new Slice { 
                          val size = slice.size; 
                          val columns = Map(ColumnRef(selector, c.tpe) -> c)
                        }
                      )
                    }
                } getOrElse {
                  (state, slice)
                } 
              }
            )
          }

        case DerefObjectStatic(source, field) =>
          composeSliceTransform(source) andThen {
            map0 { _ deref field }
          }

        case DerefObjectDynamic(source, ref) =>
          composeSliceTransform(source).zip(composeSliceTransform(ref)) { (slice, derefBy) => 
            assert(derefBy.columns.size <= 1)
            derefBy.columns.headOption collect {
              case (ColumnRef(JPath.Identity, CStringArbitrary | CStringFixed(_)), c: StrColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathField(c(row)) })
            } getOrElse {
              slice
            }
          }

        case DerefArrayStatic(source, element) =>
          composeSliceTransform(source) andThen {
            map0 { _ deref element }
          }

        case DerefArrayDynamic(source, ref) =>
          composeSliceTransform(source).zip(composeSliceTransform(ref)) { (slice, derefBy) => 
            assert(derefBy.columns.size <= 1)
            derefBy.columns.headOption collect {
              case (ColumnRef(JPath.Identity, CLong), c: LongColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathIndex(c(row).toInt) })

              case (ColumnRef(JPath.Identity, CDouble), c: DoubleColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathIndex(c(row).toInt) })

              case (ColumnRef(JPath.Identity, CDecimalArbitrary), c: NumColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathIndex(c(row).toInt) })
            } getOrElse {
              slice
            }
          }

        case ArraySwap(source, index) =>
          composeSliceTransform(source) andThen {
            map0 { _ arraySwap index }
          }
      }
    }
    
    /**
     * Performs a one-pass transformation of the keys and values in the table.
     * If the key transform is not identity, the resulting table will have
     * unknown sort order.
     */
    def transform(spec: TransSpec1): Table = {
      transform0(composeSliceTransform(spec))
    }
    
    /**
     * Cogroups this table with another table, using equality on the specified
     * transformation on rows of the table.
     */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table = sys.error("todo")
    
    /**
     * Performs a full cartesian cross on this table with the specified table,
     * applying the specified transformation to merge the two tables into
     * a single table.
     */
    def cross(that: Table)(spec: TransSpec2): Table = sys.error("todo")
    
    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: SortOrder): Table = sys.error("todo")
    
    // Does this have to be fully known at every point in time?
    def schema: JType = sys.error("todo")
    
    def drop(n: Long): Table = sys.error("todo")
    
    def take(n: Long): Table = sys.error("todo")
    
    def takeRight(n: Long): Table = sys.error("todo")

    def normalize: Table = new Table(slices.filterNot(_.isEmpty))

  /*
    def cogroup(other: Table, prefixLength: Int)(merge: CogroupMerge): Table = {
      sealed trait CogroupState
      case object StepLeftCheckRight extends CogroupState
      case object StepLeftDoneRight extends CogroupState
      case object StepRightCheckLeft extends CogroupState
      case object StepRightDoneLeft extends CogroupState
      case object Cartesian extends CogroupState
      case object Done extends CogroupState

      new Table(
        idCount,
        focus ++ other.focus,
        new Iterable[Slice] {
          def iterator = new Iterator[Slice] {
            private val leftIter = self.slices.iterator
            private val rightIter = other.slices.iterator
            
            private var leftSlice = if (leftIter.hasNext) leftIter.next else null.asInstanceOf[Slice]
            private var rightSlice = if (rightIter.hasNext) rightIter.next else null.asInstanceOf[Slice]

            private var maxSliceSize = 
              if (leftSlice == null) { 
                if (rightSlice == null) 0 else rightSlice.size 
              } else { 
                if (rightSlice == null) leftSlice.size else leftSlice.size max rightSlice.size
              }

            private var leftIdx = 0 
            private var rightIdx = 0
            private var firstRightEq: Int = -1
            private var nextRight: Int = -1

            private var leftBuffer = new ArrayIntList(maxSliceSize)
            private var rightBuffer = new ArrayIntList(maxSliceSize)

            private var state: CogroupState =
              if (leftSlice == null) {
                if (rightSlice == null) Done else StepRightDoneLeft
              } else {
                if (rightSlice == null) StepLeftDoneRight else StepLeftCheckRight
              }

            private var curSlice = precomputeNext()

            def hasNext: Boolean = curSlice ne null
            
            def next: Slice = {
              val tmp = curSlice
              curSlice = precomputeNext()
              tmp
            }

            @tailrec private def precomputeNext(): Slice = {
              @inline 
              def nonEmptyLeftSlice = leftIdx < leftSlice.size

              @inline 
              def emptyLeftSlice = leftIdx >= leftSlice.size
              
              @inline 
              def nonEmptyRightSlice = rightIdx < rightSlice.size

              @inline 
              def emptyRightSlice = rightIdx >= rightSlice.size

              @inline 
              def compareIdentities = leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) 

              state match {
                case StepLeftCheckRight => 
                  if (nonEmptyLeftSlice) {
                    compareIdentities match {
                      case LT => 
                        bufferAdvanceLeft()

                      case GT =>
                        bufferAdvanceRight()
                        state = StepRightCheckLeft

                      case EQ =>
                        bufferBoth()
                        firstRightEq = rightIdx
                        state = Cartesian
                    }

                    precomputeNext()
                  } else {
                    val result = emitSliceOnOverflow(true, false, StepRightDoneLeft)
                    if (result ne null) result else precomputeNext()
                  }

                case StepLeftDoneRight =>
                  if (nonEmptyLeftSlice) {
                    bufferAdvanceLeft()
                    precomputeNext()
                  } else {
                    val result = emitSliceOnOverflow(true, false, Done)
                    if (result ne null) result else precomputeNext()
                  }

                case StepRightCheckLeft => 
                  if (nonEmptyRightSlice) {
                    compareIdentities match {
                      case LT => 
                        bufferAdvanceLeft()
                        state = StepLeftCheckRight

                      case GT =>
                        bufferAdvanceRight()

                      case EQ =>
                        bufferBoth()
                        firstRightEq = rightIdx
                        state = Cartesian
                    }

                    precomputeNext()
                  } else {
                    val result = emitSliceOnOverflow(false, true, StepLeftDoneRight)
                    if (result ne null) result else precomputeNext()
                  }

                case StepRightDoneLeft =>
                  if (nonEmptyRightSlice) {
                    bufferAdvanceRight()
                    precomputeNext()
                  } else {
                    val result = emitSliceOnOverflow(false, true, Done)
                    if (result ne null) result else precomputeNext()
                  }

                case Cartesian =>
                  @inline 
                  def ensureRightSliceNonEmpty(): Boolean = {
                    while (rightIdx >= rightSlice.size && rightIter.hasNext) rightSlice = (rightSlice append rightIter.next)
                    nonEmptyRightSlice
                  }

                  @inline 
                  def ensureLeftSliceNonEmpty(): Boolean = {
                    while (leftIdx >= leftSlice.size && leftIter.hasNext) leftSlice = (leftSlice append leftIter.next)
                    nonEmptyLeftSlice
                  }

                  rightIdx += 1
                  if (ensureRightSliceNonEmpty()) {
                    compareIdentities match {
                      case LT => 
                        nextRight = rightIdx
                        rightIdx = firstRightEq
                        leftIdx += 1

                        if (ensureLeftSliceNonEmpty()) {
                          compareIdentities match {
                            case LT => sys.error("Inputs on the left not sorted")
                            case GT => 
                              state = StepLeftCheckRight
                              rightIdx = nextRight
                              val result = emitSliceOnOverflow(false, false, null)
                              if (result ne null) result else precomputeNext()
                            
                            case EQ => 
                              bufferBoth()
                              precomputeNext()
                          }
                        } else {
                          state = StepRightDoneLeft
                          rightIdx = nextRight
                          val result = emitSliceOnOverflow(false, false, null)
                          if (result ne null) result else precomputeNext()
                        }

                      case GT => 
                        sys.error("Inputs on the right not sorted")

                      case EQ => 
                        bufferBoth()
                        precomputeNext()
                    }
                  } else {
                    rightIdx = firstRightEq
                    leftIdx += 1
                    if (ensureLeftSliceNonEmpty()) {
                      compareIdentities match {
                        case LT => sys.error("Inputs on the left not sorted")
                        case GT => 
                          state = StepLeftDoneRight
                          val result = emitSliceOnOverflow(false, false, null)
                          if (result ne null) result else precomputeNext()
                            
                        case EQ => 
                          bufferBoth()
                          precomputeNext()
                      }
                    } else {
                      state = Done
                      emitSlice()
                    }
                  }

                case Done => null.asInstanceOf[Slice]
              }
            }

            @inline private def bufferAdvanceLeft(): Unit = {
              leftBuffer.add(leftIdx)
              rightBuffer.add(-1)
              leftIdx += 1
            }

            @inline private def bufferAdvanceRight(): Unit = {
              leftBuffer.add(-1)
              rightBuffer.add(rightIdx)
              rightIdx += 1
            }

            @inline private def bufferBoth(): Unit = {
              leftBuffer.add(leftIdx)
              rightBuffer.add(rightIdx)
            }

            private def emitSliceOnOverflow(advanceLeft: Boolean, advanceRight: Boolean, advancingState: CogroupState): Slice = {
              if (leftBuffer.size >= maxSliceSize) {
                val result = emitSlice()

                if (advanceLeft) {
                  if (leftIter.hasNext) {
                    leftSlice = leftIter.next
                    leftIdx = 0
                  } else {
                    state = advancingState
                  }
                } else if (advanceRight) {
                  if (rightIter.hasNext) {
                    rightSlice = rightIter.next
                    rightIdx = 0
                  } else {
                    state = advancingState
                  }
                }

                result
              } else {
                if (advanceLeft) {
                  if (leftIter.hasNext) {
                    leftSlice = leftSlice append leftIter.next
                    null
                  } else {
                    state = advancingState
                    if (state == Done) emitSlice() else null
                  }
                } else if (advanceRight) {
                  if (rightIter.hasNext) {
                    rightSlice = rightSlice append rightIter.next
                    null
                  } else {
                    state = advancingState
                    if (state == Done) emitSlice() else null
                  }
                } else {
                  null
                } 
              }
            }

            private def emitSlice(): Slice = {
              val result = new Slice {
                private val remappedLeft  = leftSlice.remap(F1P.bufferRemap(leftBuffer))
                private val remappedRight = rightSlice.remap(F1P.bufferRemap(rightBuffer))

                val idCount = self.idCount + other.idCount
                val size = leftBuffer.size

                // merge identity columns
                val identities = {
                  val (li, lr) = remappedLeft.identities.splitAt(prefixLength)
                  val (ri, rr) = remappedRight.identities.splitAt(prefixLength)
                  val sharedPrefix = (li zip ri) map {
                    case (c1, c2) => new Column[Long] {
                      val returns = CLong
                      def isDefinedAt(row: Int) = c1.isDefinedAt(row) || c2.isDefinedAt(row)
                      def apply(row: Int) = {
                        if (c1.isDefinedAt(row)) c1(row) // identities must be equal, or c2 must be undefined
                        else if (c2.isDefinedAt(row)) c2(row) 
                        else sys.error("impossible")
                      }
                    }
                  }

                  sharedPrefix ++ lr ++ rr
                }

                val columns = remappedRight.columns.foldLeft(remappedLeft.columns) {
                  case (acc, (rref, rcol)) => 
                    val mergef = merge(rref).get

                    acc.get(rref) match {
                      case None =>
                        acc + (rref -> rcol)

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
                                lc(row)        
                              }
                            } else {
                              rc(row)
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
              leftBuffer = new ArrayIntList(maxSliceSize)
              rightBuffer = new ArrayIntList(maxSliceSize)
              result
            }
          }
        }
      )
    }
    */

    
    def toJson: Iterable[JValue] = {
      toEvents { (slice: Slice, row: RowId) => slice.toJson(row) }
    }

    private def toEvents[A](f: (Slice, RowId) => A): Iterable[A] = {
      new Iterable[A] {
        def iterator = {
          val normalized = self.normalize.slices.iterator

          new Iterator[A] {
            private var slice = if (normalized.hasNext) normalized.next else null.asInstanceOf[Slice]
            private var idx = 0
            private var next0: A = precomputeNext()

            def hasNext = next0 != null

            def next() = {
              val tmp = next0
              next0 = precomputeNext()
              tmp
            }
           
            @tailrec def precomputeNext(): A = {
              if (slice == null) {
                null.asInstanceOf[A]
              } else if (idx < slice.size) {
                val result = f(slice, idx)
                idx += 1
                result
              } else {
                slice = if (normalized.hasNext) normalized.next else null.asInstanceOf[Slice]
                idx = 0
                precomputeNext() //recursive call is okay because hasNext must have returned true to get here
              }
            }
          }
        }
      }
    }
  }
  
  object Table {
    def fromJson(values: Iterable[JValue]): Table = {
      val sliceSize = 10      // ???
      
      def makeSlice(sampleData: Iterable[JValue]): (Slice, Iterable[JValue]) = {
        val (prefix, suffix) = sampleData.splitAt(sliceSize)
    
        @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, (BitSet, Array[_])], sliceIndex: Int): (Map[ColumnRef, (BitSet, Object)], Int) = {
          from match {
            case jv #:: xs =>
              val withIdsAndValues = jv.flattenWithPath.foldLeft(into) {
                case (acc, (jpath, JNothing)) => acc
                case (acc, (jpath, v)) =>
                  val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
                  val ref = ColumnRef(jpath, ctype)
    
                  val pair: (BitSet, Array[_]) = v match {
                    case JBool(b) => 
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Boolean](sliceSize))).asInstanceOf[(BitSet, Array[Boolean])]
                      col(sliceIndex) = b
                      (defined + sliceIndex, col)
    
                    case JInt(ji) => CType.sizedIntCValue(ji) match {
                      case CLong(v) =>
                        val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Long](sliceSize))).asInstanceOf[(BitSet, Array[Long])]
                        col(sliceIndex) = v
                        (defined + sliceIndex, col)
    
                      case CNum(v) =>
                        val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[BigDecimal](sliceSize))).asInstanceOf[(BitSet, Array[BigDecimal])]
                        col(sliceIndex) = v
                        (defined + sliceIndex, col)
                    }
    
                    case JDouble(d) => 
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Double](sliceSize))).asInstanceOf[(BitSet, Array[Double])]
                      col(sliceIndex) = d
                      (defined + sliceIndex, col)
    
                    case JString(s) => 
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[String](sliceSize))).asInstanceOf[(BitSet, Array[String])]
                      col(sliceIndex) = s
                      (defined + sliceIndex, col)
                    
                    case JArray(Nil)  => 
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                      (defined + sliceIndex, col)
    
                    case JObject(Nil) => 
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                      (defined + sliceIndex, col)
    
                    case JNull        => 
                      val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                      (defined + sliceIndex, col)
                  }
    
                  acc + (ref -> pair)
              }
    
              buildColArrays(xs, withIdsAndValues, sliceIndex + 1)
    
            case _ => (into, sliceIndex)
          }
        }
    
        // FIXME: If prefix is empty (eg. because sampleData.data is empty) the generated
        // columns won't satisfy sampleData.schema. This will cause the subsumption test in
        // Slice#typed to fail unless it allows for vacuous success
        val slice = new Slice {
          val (cols, size) = buildColArrays(prefix.toStream, Map.empty[ColumnRef, (BitSet, Array[_])], 0) 
          val columns = cols map {
            case (ref @ ColumnRef(_, CBoolean), (defined, values))          => (ref, ArrayBoolColumn(defined, values.asInstanceOf[Array[Boolean]]))
            case (ref @ ColumnRef(_, CLong), (defined, values))             => (ref, ArrayLongColumn(defined, values.asInstanceOf[Array[Long]]))
            case (ref @ ColumnRef(_, CDouble), (defined, values))           => (ref, ArrayDoubleColumn(defined, values.asInstanceOf[Array[Double]]))
            case (ref @ ColumnRef(_, CDecimalArbitrary), (defined, values)) => (ref, ArrayNumColumn(defined, values.asInstanceOf[Array[BigDecimal]]))
            case (ref @ ColumnRef(_, CStringArbitrary), (defined, values))  => (ref, ArrayStrColumn(defined, values.asInstanceOf[Array[String]]))
            case (ref @ ColumnRef(_, CEmptyArray), (defined, values))       => (ref, new BitsetColumn(defined) with EmptyArrayColumn)
            case (ref @ ColumnRef(_, CEmptyObject), (defined, values))      => (ref, new BitsetColumn(defined) with EmptyObjectColumn)
            case (ref @ ColumnRef(_, CNull), (defined, values))             => (ref, new BitsetColumn(defined) with NullColumn)
          }
        }
    
        (slice, suffix)
      }
      
      val (s, xs) = makeSlice(values)
      
      new Table(new Iterable[Slice] {
        def iterator = new Iterator[Slice] {
          private var _next = s
          private var _rest = xs
  
          def hasNext = _next != null
          def next() = {
            val tmp = _next
            _next = if (_rest.isEmpty) null else {
              val (s, xs) = makeSlice(_rest)
              _rest = xs
              s
            }
            tmp
          }
        }
      })
    }
  }
}
// vim: set ts=4 sw=4 et:
