package com.precog.yggdrasil
package table

import util.CPathUtils

import com.precog.common.VectorCase
import com.precog.bytecode._
import com.precog.util._

import com.precog.common.json._

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList

import scala.annotation.tailrec
import scala.collection.{breakOut, BitSet, mutable}
import scalaz._
import scalaz.Ordering._
import scalaz.Validation._
import scalaz.syntax.foldable._
import scalaz.syntax.semigroup._
import scalaz.std.iterable._

trait RowComparator { self =>
  def compare(i1: Int, i2: Int): Ordering

  def swap: RowComparator = new RowComparator {
    def compare(i1: Int, i2: Int) = self.compare(i2, i1).complement
  }

  @tailrec
  final def nextLeftIndex(lmin: Int, lmax: Int, ridx: Int): Int = {
    compare(lmax, ridx) match {
      case LT => lmax + 1
      case GT => 
        if (lmax - lmin <= 1) {
          compare(lmin, ridx) match {
            case LT => lmax
            case GT | EQ => lmin
          }
        } else {
          val lmid = lmin + ((lmax - lmin) / 2)
          compare(lmid, ridx) match {
            case LT => nextLeftIndex(lmid + 1, lmax, ridx)
            case GT | EQ => nextLeftIndex(lmin, lmid - 1, ridx)
          }
        }
    
      case EQ => 
        if (lmax - lmin <= 1) {
          compare(lmin, ridx) match {
            case LT => lmax
            case GT | EQ => lmin
          }
        } else {
          val lmid = lmin + ((lmax - lmin) / 2)
          compare(lmid, ridx) match {
            case LT => nextLeftIndex(lmid + 1, lmax, ridx)
            case GT => sys.error("inputs on the left not sorted.")
            case EQ => nextLeftIndex(lmin, lmid - 1, ridx)
          }
        }
    }
  }
}

trait Slice { source =>
  import Slice._
  import TableModule._

  def size: Int
  def isEmpty: Boolean = size == 0
  def nonEmpty = !isEmpty

  def columns: Map[ColumnRef, Column]

  def logicalColumns: JType => Set[Column] = { jtpe =>
    columns collect {
      case (ColumnRef(jpath, ctype), col) if Schema.includes(jtpe, jpath, ctype) => col
    } toSet
  }

  lazy val valueColumns: Set[Column] = columns collect { case (ColumnRef(CPath.Identity, _), col) => col } toSet
  
  def isDefinedAt(row: Int) = columns.values.exists(_.isDefinedAt(row))

  def mapRoot(f: CF1): Slice = new Slice {
    val size = source.size

    val columns: Map[ColumnRef, Column] = {
      val resultColumns = for {
        col <- source.columns collect { case (ref, col) if ref.selector == CPath.Identity => col }
        result <- f(col)
      } yield result

      resultColumns.groupBy(_.tpe) map { 
        case (tpe, cols) => (ColumnRef(CPath.Identity, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      }
    }
  }

  def mapColumns(f: CF1): Slice = new Slice {
    val size = source.size

    val columns: Map[ColumnRef, Column] = {
      val resultColumns: Map[ColumnRef, Column] = for {
        (ref, col) <- source.columns
        result <- f(col)
      } yield (ref.copy(ctype = result.tpe), result)

      resultColumns.groupBy(_._1) map {
        case (ref, pairs) => (ref, pairs.map(_._2).reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      }
    }
  }

  /**
   * Transform this slice such that its columns are only defined for row indices
   * in the given BitSet.
   */
  def redefineWith(s: BitSet): Slice = mapColumns(cf.util.filter(0, size, s))
  
  def definedConst(value: CValue): Slice = new Slice {
    val size = source.size
    val columns = {
      Map(
        value match {
          case CString(s) => (ColumnRef(CPath.Identity, CString), new StrColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = s
          })
          case CBoolean(b) => (ColumnRef(CPath.Identity, CBoolean), new BoolColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = b
          })
          case CLong(l) => (ColumnRef(CPath.Identity, CLong), new LongColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = l
          })
          case CDouble(d) => (ColumnRef(CPath.Identity, CDouble), new DoubleColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = d
          })
          case CNum(n) => (ColumnRef(CPath.Identity, CNum), new NumColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = n
          })
          case CDate(d) => (ColumnRef(CPath.Identity, CDate), new DateColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = d
          })
          case CNull => (ColumnRef(CPath.Identity, CNull), new NullColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
          case CEmptyObject => (ColumnRef(CPath.Identity, CEmptyObject), new EmptyObjectColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
          case CEmptyArray => (ColumnRef(CPath.Identity, CEmptyArray), new EmptyArrayColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
          case CUndefined => sys.error("Cannot define a constant undefined value")
        }
      )
    }
  }

  def deref(node: CPathNode): Slice = {
    new Slice {
      val size = source.size
      val columns = source.columns.collect {
        case (ColumnRef(CPath(`node`, xs @ _*), ctype), col) => (ColumnRef(CPath(xs: _*), ctype), col)
      }
    }
  }

  def wrap(wrapper: CPathNode): Slice = {
    new Slice {
      val size = source.size
      val columns = source.columns.map {
        case (ColumnRef(CPath(nodes @ _*), ctype), col) => (ColumnRef(CPath(wrapper +: nodes : _*), ctype), col)
      }
    }
  }

  def delete(jtype: JType): Slice = new Slice {
    def fixArrays(columns: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
      columns.toSeq.sortBy(_._1).foldLeft((Map.empty[Vector[CPathNode], Int], Map.empty[ColumnRef, Column])) {
        case ((arrayPaths, acc), (ColumnRef(jpath, ctype), col)) => 
          val (arrayPaths0, nodes) = jpath.nodes.foldLeft((arrayPaths, Vector.empty[CPathNode])) {
            case ((ap, nodes), CPathIndex(_)) => 
              val idx = ap.getOrElse(nodes, -1) + 1
              (ap + (nodes -> idx), nodes :+ CPathIndex(idx))

            case ((ap, nodes), fieldNode) => (ap, nodes :+ fieldNode)
          }

          (arrayPaths0, acc + (ColumnRef(CPath(nodes: _*), ctype) -> col))
      }._2
    }
    
    val size = source.size
    val columns = fixArrays(
      source.columns.filterNot {
        case (ColumnRef(selector, ctype), _) => Schema.includes(jtype, selector, ctype)
      }
    )
  }

  def deleteFields(prefixes: scala.collection.Set[CPathField]) = {
    new Slice {
      val size = source.size
      val columns = source.columns filterNot {
        case (ColumnRef(CPath(head @ CPathField(_), _ @ _*), _), _) => prefixes contains head
        case _ => false
      }
    }
  }

  def typed(jtpe: JType): Slice = {
    new Slice {  
      val size = source.size
      val columns = source.columns.filter { case (ColumnRef(path, ctpe), _) => Schema.includes(jtpe, path, ctpe) } 
    }
  }

  def nest(selectorPrefix: CPath) = new Slice {
    val size = source.size
    val columns = source.columns map { case (ColumnRef(selector, ctype), v) => ColumnRef(selectorPrefix \ selector, ctype) -> v }
  }

  def arraySwap(index: Int) = new Slice {
    val size = source.size
    val columns = source.columns.collect {
      case (ColumnRef(CPath(CPathIndex(0), xs @ _*), ctype), col) => 
        (ColumnRef(CPath(CPathIndex(index) +: xs : _*), ctype), col)

      case (ColumnRef(CPath(CPathIndex(`index`), xs @ _*), ctype), col) => 
        (ColumnRef(CPath(CPathIndex(0) +: xs : _*), ctype), col)

      case c @ (ColumnRef(CPath(CPathIndex(i), xs @ _*), ctype), col) => c
    }
  }

  // Takes an array where the indices correspond to indices in this slice,
  // and the values give the indices in the sparsened slice.
  def sparsen(index: Array[Int], toSize: Int): Slice = new Slice {
    val size = toSize
    val columns = source.columns mapValues { col => 
      cf.util.Sparsen(index, toSize)(col).get //sparsen is total
    }
  }

  def remap(indices: ArrayIntList) = new Slice {
    val size = indices.size
    val columns: Map[ColumnRef, Column] = source.columns mapValues { col => 
      cf.util.RemapIndices(indices).apply(col).get
    }
  }

  def map(from: CPath, to: CPath)(f: CF1): Slice = new Slice {
    val size = source.size

    val columns: Map[ColumnRef, Column] = {
      val resultColumns = for {
        col <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(from) => col }
        result <- f(col)
      } yield result

      resultColumns.groupBy(_.tpe) map { 
        case (tpe, cols) => (ColumnRef(to, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      }
    }
  }

  def map2(froml: CPath, fromr: CPath, to: CPath)(f: CF2): Slice = new Slice {
    val size = source.size

    val columns: Map[ColumnRef, Column] = {
      val resultColumns = for {
        left   <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(froml) => col }
        right  <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(fromr) => col }
        result <- f(left, right)
      } yield result

      resultColumns.groupBy(_.tpe) map { case (tpe, cols) => (ColumnRef(to, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2))) }
    }
  }

  def filterDefined(filter: Slice, definedness: Definedness) = {
    new Slice {
      private val colValues = filter.columns.values
      private val defined = definedness match {
        case AnyDefined =>
          (0 until source.size).foldLeft(new mutable.BitSet()) {
            case (acc, i) => if (colValues.exists(_.isDefinedAt(i))) acc + i else acc
          }

        case AllDefined =>
          (0 until source.size).foldLeft(new mutable.BitSet()) {
            case (acc, i) => if (colValues.nonEmpty && colValues.forall(_.isDefinedAt(i))) acc + i else acc
          }
      }

      val size = source.size
      val columns: Map[ColumnRef, Column] = source.columns mapValues { col => cf.util.filter(0, source.size, defined)(col).get }
    }
  }

  def compact(filter: Slice, definedness: Definedness): Slice = {
    new Slice {
      lazy val retained = definedness match {
        case AnyDefined =>
          (0 until filter.size).foldLeft(new ArrayIntList) {
            case (acc, i) => if (filter.columns.values.exists(_.isDefinedAt(i))) acc.add(i) ; acc
          }

        case AllDefined =>
          (0 until filter.size).foldLeft(new ArrayIntList) {
            case (acc, i) =>
              if (filter.columns.values.forall(_.isDefinedAt(i))) acc.add(i)
              acc
          }
      }

      lazy val size = retained.size
      lazy val columns: Map[ColumnRef, Column] = source.columns mapValues {
        col => (col |> cf.util.RemapIndices(retained)).get
      }
    }
  }

  def retain(refs: Set[ColumnRef]) = {
    new Slice {
      val size = source.size
      val columns: Map[ColumnRef, Column] = source.columns.filterKeys(refs)
    }
  }
  
  /**
   * Assumes that this and the previous slice (if any) are sorted.
   */
  def distinct(prevFilter: Option[Slice], filter: Slice): Slice = {
    new Slice {
      lazy val retained : ArrayIntList = {
        val acc = new ArrayIntList
        
        def findSelfDistinct(prevRow: Int, curRow: Int) = {
          val selfComparator = rowComparatorFor(filter, filter)(_.columns.keys.toList.sorted)
        
          @tailrec
          def findSelfDistinct0(prevRow: Int, curRow: Int) : ArrayIntList = {
            if(curRow >= filter.size) acc
            else {
              val retain = selfComparator.compare(prevRow, curRow) != EQ
              if(retain) acc.add(curRow)
              findSelfDistinct0(if(retain) curRow else prevRow, curRow+1)
            }
          }
          
          findSelfDistinct0(prevRow, curRow)
        }

        def findStraddlingDistinct(prev: Slice, prevRow: Int, curRow: Int) = {
          val straddleComparator = rowComparatorFor(prev, filter)(_.columns.keys.toList.sorted) 

          @tailrec
          def findStraddlingDistinct0(prevRow: Int, curRow: Int): ArrayIntList = {
            if(curRow >= filter.size) acc
            else {
              val retain = straddleComparator.compare(prevRow, curRow) != EQ
              if(retain) acc.add(curRow)
              if(retain)
                findSelfDistinct(curRow, curRow+1)
              else
                findStraddlingDistinct0(prevRow, curRow+1)
            }
          }

          findStraddlingDistinct0(prevRow, curRow)
        }
        
        val lastDefined = prevFilter.flatMap { slice =>
          (slice.size-1 to 0 by -1).find(row => slice.columns.values.exists(_.isDefinedAt(row))) }.map {
            (prevFilter.get, _)
          }
        
        val firstDefined = (0 until filter.size).find(i => filter.columns.values.exists(_.isDefinedAt(i)))

        (lastDefined, firstDefined) match {
          case (Some((prev, i)), Some(j)) => findStraddlingDistinct(prev, i, j)
          case (_,               Some(j)) => acc.add(j) ; findSelfDistinct(j, j+1)
          case _                          => acc
        }
      }

      lazy val size = retained.size
      lazy val columns: Map[ColumnRef, Column] = source.columns mapValues {
        col => (col |> cf.util.RemapIndices(retained)).get
      }
    }
  }

  def sortBy(refs: VectorCase[CPath]): Slice = {
    val sortedIndices: Array[Int] = {
      import java.util.Arrays
      val arr = Array.range(0, source.size)

      val comparator = new IntOrder {
        def order(i1: Int, i2: Int) = {
          var i = 0
          var result: Ordering = EQ
          //while (i < accessors.length && (result eq EQ)) {
            sys.error("todo")
          //}
          result
        }
      }

      Slice.qsort(arr, comparator)
      arr
    }

    source mapRoot cf.util.Remap(sortedIndices)
  }

  /**
   * Split the table at the specified index, exclusive. The
   * new prefix will contain all indices less than that index, and
   * the new suffix will contain indices >= that index.
   */
  def split(idx: Int): (Slice, Slice) = {
    (take(idx), drop(idx))
  }

  def take(sz: Int): Slice = if (sz >= source.size) source else {
    new Slice {
      val size = sz
      val columns = source.columns mapValues {
        col => (col |> cf.util.RemapFilter(_ < sz, 0)).get
      }
    }
  }

  def drop(sz: Int): Slice = if (sz <= 0) source else {
    new Slice {
      val size = source.size - sz
      val columns = source.columns mapValues {
        col => (col |> cf.util.RemapFilter(_ < size, sz)).get
      }
    }
  }

  def takeRange(startIndex: Int, numberToTake: Int): Slice = {
    new Slice {
      val size = numberToTake
      val columns = source.columns mapValues { 
        col => (col |> cf.util.RemapFilter(_ < numberToTake, startIndex)).get
      }
    }
  }

  def append(other: Slice): Slice = {
    new Slice {
      val size = source.size + other.size
      val columns = other.columns.foldLeft(source.columns) {
        case (acc, (ref, col)) => 
          val appendedCol = acc.get(ref) flatMap { sc => 
            cf.util.Concat(source.size)(sc, col)
          } getOrElse {
            (col |> cf.util.Shift(source.size)).get
          }

          acc + (ref -> appendedCol)
      }
    }
  }

  def zip(other: Slice): Slice = {
    new Slice {
      val size = source.size max other.size
      val columns: Map[ColumnRef, Column] = other.columns.foldLeft(source.columns) {
        case (acc, (ref, col)) => acc + (ref -> (acc get ref flatMap { c => cf.util.UnionRight(c, col) } getOrElse col))
      }
    }
  }

  def toJson(row: Int): Option[JValue] = {
    columns.foldLeft[JValue](JNothing) {
      case (jv, (ColumnRef(selector, _), col)) if col.isDefinedAt(row) =>
        CPathUtils.cPathToJPaths(selector, col.cValue(row)).foldLeft(jv) {
          case (jv, (path, value)) => jv.unsafeInsert(path, value.toJValue)
        }

      case (jv, _) => jv
    } match {
      case JNothing => None
      case jv       => Some(jv)
    }
  }

  def toString(row: Int): Option[String] = {
    (columns.toList.sortBy(_._1) map { case (ref, col) => ref.toString + ": " + (if (col.isDefinedAt(row)) col.strValue(row) else "(undefined)") }) match {
      case Nil => None
      case l   => Some(l.mkString("[", ", ", "]")) 
    }
  }

  def toJsonString: String = (0 until size).map(toJson).mkString("\n")

  override def toString = (0 until size).map(toString(_).getOrElse("")).mkString("\n")
}

object Slice {
  def apply(columns0: Map[ColumnRef, Column], dataSize: Int) = {
    new Slice {
      val size = dataSize
      val columns = columns0
    }
  }

  // scalaz order isn't @specialized
  trait IntOrder {
    def order(i1: Int, i2: Int): Ordering
  }

  private val MIN_QSORT_SIZE = 7; 

  def qsort(x: Array[Int], ord: IntOrder): Unit = {
    val random = new java.util.Random();
    qsortPartial(x, 0, x.length-1, ord, random);
    isort(x, ord);
  }

  private def isort(x: Array[Int], ord: IntOrder): Unit = {
    @tailrec def sort(i: Int): Unit = if (i < x.length) {
      val t = x(i);
      var j = i;
      while(j > 0 && (ord.order(t, x(j-1)) eq LT)) { x(j) = x(j-1); j -= 1 } 
      x(j) = t;
      sort(i + 1)
    }

    sort(0)
  }

  private def qsortPartial(x: Array[Int], lower: Int, upper: Int, ord: IntOrder, random: java.util.Random): Unit = {
    if (upper - lower >= MIN_QSORT_SIZE) {
      swap(x, lower, lower + random.nextInt(upper-lower+1));
      val t = x(lower);
      var i = lower;
      var j = upper + 1;
      var cont = true
      while (cont) {
        do { i += 1 } while (i <= upper && (ord.order(x(i), t) eq LT))
        do { j -= 1 } while (ord.order(t, x(j)) eq LT)
        if (i > j) cont = false
        if (i < upper) swap(x, i, j)
      }
    }
  }

  @inline 
  private def swap(xs: Array[Int], i: Int, j: Int) {
    val temp = xs(i);
    xs(i) = xs(j);
    xs(j) = temp;
  }

  def rowComparatorFor(s1: Slice, s2: Slice)(keyf: Slice => List[ColumnRef]): RowComparator = {
    def compare0(cols: (Column, Column)): RowComparator = {
      cols match {
        case (c1: BoolColumn, c2: BoolColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow) 
            if (thisVal == c2(thatRow)) EQ else if (thisVal) GT else LT
          }
        }

        case (c1: LongColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: LongColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: LongColumn, c2: NumColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: DoubleColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: DoubleColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: DoubleColumn, c2: NumColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: NumColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: NumColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: NumColumn, c2: NumColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            NumericComparisons.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: StrColumn, c2: StrColumn) => new RowComparator {
          val ord = Order[String]
          def compare(thisRow: Int, thatRow: Int) = {
            ord.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: DateColumn, c2: DateColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal isAfter thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1, c2) => {
          val ordering = implicitly[Order[CType]].apply(c1.tpe, c2.tpe)
          
          // This also correctly catches CNullType cases.

          new RowComparator {
            def compare(thisRow: Int, thatRow: Int) = ordering
          }
        }
      }
    }

    val refs1 = keyf(s1)
    val refs2 = keyf(s2)

    // Return the first column in the array defined at the row, or -1 if none are defined for that row
    @inline def firstDefinedIndexFor(columns: Array[Column], row: Int): Int = {
      var i = 0
      while (i < columns.length && ! columns(i).isDefinedAt(row)) { i += 1 }
      if (i == columns.length) -1 else i
    }

    @inline def genComparatorFor(l1: List[ColumnRef], l2: List[ColumnRef]): RowComparator = {
      new RowComparator {
        private val array1 = l1.map(s1.columns).toArray
        private val array2 = l2.map(s2.columns).toArray

        // Build an array of pairwise comparator functions for later use
        private val comparators: Array[RowComparator] = (for {
          i1 <- 0 until array1.length
          i2 <- 0 until array2.length
        } yield compare0(array1(i1), array2(i2)))(collection.breakOut)

        def compare(i: Int, j: Int) = {
          val first1 = firstDefinedIndexFor(array1, i)
          val first2 = firstDefinedIndexFor(array2, j)

          // In the following, undefined always sorts LT defined values
          if (first1 == -1 && first2 == -1) {
            EQ
          } else if (first1 == -1) {
            LT
          } else if (first2 == -1) {
            GT
          } else {
            // We have the indices, so use it to look up the comparator for the rows
            comparators(first1 * array2.length + first2).compare(i, j)
          }
        }
      }
    }

    @inline @tailrec
    def pairColumns(l1: List[ColumnRef], l2: List[ColumnRef], comparators: List[RowComparator]): List[RowComparator] = {
      import scalaz.syntax.order._

      (l1, l2) match {
        case (h1 :: t1, h2 :: t2) if h1.selector == h2.selector => {
          val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)
          val (l2Equal, l2Rest) = l2.partition(_.selector == h2.selector)

          pairColumns(l1Rest, l2Rest, genComparatorFor(l1Equal, l2Equal) :: comparators)
        }

        case (h1 :: t1, h2 :: t2) if h1 ?|? h2 == LT => {
          val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)

          pairColumns(l1Rest, l2, genComparatorFor(l1Equal, Nil) :: comparators)
        }

        case (h1 :: t1, h2 :: t2) if h1 ?|? h2 == GT => {
          val (l2Equal, l2Rest) = l2.partition(_.selector == h2.selector)

          pairColumns(l1, l2Rest, genComparatorFor(Nil, l2Equal) :: comparators)
        }

        case (h1 :: t1, Nil) => {
          val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)

          pairColumns(l1Rest, Nil, genComparatorFor(l1Equal, Nil) :: comparators)
        }

        case (Nil, h2 :: t2) => {
          val (l2Equal, l2Rest) = l2.partition(_.selector == h2.selector)

          pairColumns(Nil, l2Rest, genComparatorFor(Nil, l2Equal) :: comparators)
        }

        case (Nil, Nil) => comparators.reverse

        case (h1 :: t1, h2 :: t2) => sys.error("selector guard failure in pairColumns")
      }
    }

    val comparators: Array[RowComparator] = pairColumns(refs1, refs2, Nil).toArray

    new RowComparator {
      def compare(i1: Int, i2: Int) = {
        var i = 0
        var result: Ordering = EQ

        while (i < comparators.length && result == EQ) {
          result = comparators(i).compare(i1, i2)
          i += 1
        }
        
        result
      }
    }
  } 
}
