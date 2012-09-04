package com.precog.yggdrasil
package table

import com.precog.common.VectorCase
import com.precog.bytecode.JType

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList

import scala.annotation.tailrec
import scala.collection.{breakOut, BitSet}
import scalaz._
import scalaz.Ordering._
import scalaz.Validation._
import scalaz.syntax.foldable._
import scalaz.syntax.semigroup._
import scalaz.std.iterable._

trait RowComparator { self =>
  def compare(i1: Int, i2: Int): Ordering

  def swap: RowComparator = new RowComparator {
    def compare(i1: Int, i2: Int) = self.compare(i2, i1)
  }

  @tailrec
  final def nextLeftIndex(lidx: Int, lsize: Int, ridx: Int, step: Int): Int = {
    if (lidx < lsize) {
      compare(lidx, ridx) match {
        case EQ | GT =>
          if (step <= 1) lidx -1
          nextLeftIndex(lidx - (step / 2), lsize, ridx, step / 2)

        case LT => 
          nextLeftIndex(lidx + step, lsize, ridx, step)
      }
    } else {
      lsize
    }
  }
}

trait Slice { source =>
  import Slice._
  import TableModule._

  def size: Int
  def isEmpty: Boolean = size == 0

  def columns: Map[ColumnRef, Column]

  def logicalColumns: JType => Set[Column] = { jtpe =>
    columns collect {
      case (ColumnRef(jpath, ctype), col) if Schema.includes(jtpe, jpath, ctype) => col
    } toSet
  }

  lazy val valueColumns: Set[Column] = columns collect { case (ColumnRef(JPath.Identity, _), col) => col } toSet
  
  def isDefinedAt(row: Int) = columns.values.exists(_.isDefinedAt(row))

  // FIXME: rename to mapRoot
  def mapColumns(f: CF1): Slice = new Slice {
    val size = source.size
    val columns = source.columns flatMap {
      case (ref, col) => 
        if (ref.selector == JPath.Identity) f(col) map { (ref, _ ) }  
        else None
    }
  }

  // FIXME: rename to mapColumns
  def filterColumns(f: CF1): Slice = new Slice {
    val size = source.size
    val columns = source.columns flatMap {
      case (ref, col) => f(col) map { (ref, _ ) }  
    }
  }

  /**
   * Transform this slice such that its columns are only defined for row indices
   * in the given BitSet.
   */
  def redefineWith(s: BitSet): Slice = filterColumns(cf.util.filter(0, size - 1, s))
  
  def definedConst(value: CValue): Slice = new Slice {
    val size = source.size
    val columns = {
      Map(
        value match {
          case CString(s) => (ColumnRef(JPath.Identity, CString), new StrColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = s
          })
          case CBoolean(b) => (ColumnRef(JPath.Identity, CBoolean), new BoolColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = b
          })
          case CLong(l) => (ColumnRef(JPath.Identity, CLong), new LongColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = l
          })
          case CDouble(d) => (ColumnRef(JPath.Identity, CDouble), new DoubleColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = d
          })
          case CNum(n) => (ColumnRef(JPath.Identity, CNum), new NumColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = n
          })
          case CDate(d) => (ColumnRef(JPath.Identity, CDate), new DateColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = d
          })
          case CNull => (ColumnRef(JPath.Identity, CNull), new NullColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
          case CEmptyObject => (ColumnRef(JPath.Identity, CEmptyObject), new EmptyObjectColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
          case CEmptyArray => (ColumnRef(JPath.Identity, CEmptyArray), new EmptyArrayColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
        }
      )
    }
  }

  def deref(node: JPathNode): Slice = new Slice {
    val size = source.size
    val columns = source.columns.collect {
      // case (ColumnRef(JPath(`node` :: rest), ctype), col) => (ColumnRef(JPath(rest), ctype), col) // TODO: why won't this work?
      case (ColumnRef(JPath(`node`, xs @ _*), ctype), col) => (ColumnRef(JPath(xs: _*), ctype), col)
    }
  }

  def wrap(wrapper: JPathNode): Slice = new Slice {
    val size = source.size
    val columns = source.columns.map {
      case (ColumnRef(JPath(nodes @ _*), ctype), col) => (ColumnRef(JPath(wrapper +: nodes : _*), ctype), col)
    }
  }

  def delete(jtype: JType): Slice = new Slice {
    def fixArrays(columns: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
      columns.toSeq.sortBy(_._1).foldLeft((Map.empty[Vector[JPathNode], Int], Map.empty[ColumnRef, Column])) {
        case ((arrayPaths, acc), (ColumnRef(jpath, ctype), col)) => 
          val (arrayPaths0, nodes) = jpath.nodes.foldLeft((arrayPaths, Vector.empty[JPathNode])) {
            case ((ap, nodes), JPathIndex(_)) => 
              val idx = ap.getOrElse(nodes, -1) + 1
              (ap + (nodes -> idx), nodes :+ JPathIndex(idx))

            case ((ap, nodes), fieldNode) => (ap, nodes :+ fieldNode)
          }

          (arrayPaths0, acc + (ColumnRef(JPath(nodes: _*), ctype) -> col))
      }._2
    }
    
    val size = source.size
    val columns = fixArrays(
      source.columns.filterNot {
        case (ColumnRef(selector, ctype), _) => Schema.includes(jtype, selector, ctype)
      }
    )
  }

  def deleteFields(prefixes: scala.collection.Set[JPathField]) = {
    new Slice {
      val size = source.size
      val columns = source.columns filterNot {
        case (ColumnRef(JPath(head @ JPathField(_), _ @ _*), _), _) => prefixes contains head
        case _ => false
      }
    }
  }

  def typed(jtpe : JType) : Slice = new Slice {
    val size = source.size
    val columns = {
      if(size == 0 || Schema.subsumes(source.columns.map { case (ColumnRef(path, ctpe), _) => (path, ctpe) }(breakOut), jtpe))
        source.columns.filter { case (ColumnRef(path, ctpe), _) => Schema.includes(jtpe, path, ctpe) }
      else
        Map.empty[ColumnRef, Column]
    }
  }

  def nest(selectorPrefix: JPath) = new Slice {
    val size = source.size
    val columns = source.columns map { case (ColumnRef(selector, ctype), v) => ColumnRef(selectorPrefix \ selector, ctype) -> v }
  }

  def arraySwap(index: Int) = new Slice {
    val size = source.size
    val columns = source.columns.collect {
      case (ColumnRef(JPath(JPathIndex(0), xs @ _*), ctype), col) => 
        (ColumnRef(JPath(JPathIndex(index) +: xs : _*), ctype), col)

      case (ColumnRef(JPath(JPathIndex(`index`), xs @ _*), ctype), col) => 
        (ColumnRef(JPath(JPathIndex(0) +: xs : _*), ctype), col)

      case unchanged => unchanged
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
      cf.util.Remap.forIndices(indices).apply(col).get //Remap is total
    }
  }

  def map(from: JPath, to: JPath)(f: CF1): Slice = new Slice {
    val size = source.size
    val columns = source.columns flatMap {
                    case (ref, col) if ref.selector.hasPrefix(from) => f(col) map {v => (ref, v)}
                    case unchanged => Some(unchanged)
                  }
  }

  def map2(froml: JPath, fromr: JPath, to: JPath)(f: CF2): Slice = new Slice {
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

  def compact(filter: Slice, definedness: Definedness): Slice = {
    new Slice {
      lazy val retained = definedness match {
        case AnyDefined =>
          (0 until filter.size).foldLeft(new ArrayIntList) {
            case (acc, i) => if (filter.columns.values.exists(_.isDefinedAt(i))) acc.add(i) ; acc
          }

        case AllDefined =>
          (0 until filter.size).foldLeft(new ArrayIntList) {
            case (acc, i) => if (filter.columns.values.forall(_.isDefinedAt(i))) acc.add(i) ; acc
          }
      }

      lazy val size = retained.size
      lazy val columns: Map[ColumnRef, Column] = source.columns mapValues { col => (col |> cf.util.Remap.forIndices(retained)).get }
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
      lazy val columns: Map[ColumnRef, Column] = source.columns mapValues { col => (col |> cf.util.Remap.forIndices(retained)).get }
    }
  }

  def sortBy(refs: VectorCase[JPath]): Slice = {
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

    source mapColumns cf.util.Remap(sortedIndices)
  }

  def split(idx: Int): (Slice, Slice) = (
    new Slice {
      val size = idx
      val columns = source.columns mapValues { col => (col |> cf.util.Remap({case i if i < idx => i})).get }
    },
    new Slice {
      val size = source.size - idx
      val columns = source.columns mapValues { col => (col |> cf.util.Remap({case i if i < size => i + idx})).get }
    }
  )

  def takeRange(startIndex: Int, numberToTake: Int): Slice = {
    new Slice {
      val size = numberToTake
      val columns = source.columns mapValues { 
        col => (col |> cf.util.Remap( { case i if i < numberToTake => i + startIndex} )).get 
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
      case (jv, (ref @ ColumnRef(selector, _), col)) if col.isDefinedAt(row) => {
        jv.unsafeInsert(selector, col.jValue(row))
      }

      case (jv, _) => jv
    } match {
      case JNothing => None
      case jv       => Some(jv)
    }
  }

  def toString(row: Int): Option[String] = {
    (columns collect { case (ref, col) if col.isDefinedAt(row) => ref.toString + ": " + col.strValue(row) }) match {
      case Nil => None
      case l   => Some(l.mkString("[", ", ", "]")) 
    }
  }

  override def toString = (0 until size).map(toString).mkString("\n")
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
        swap(x, i, j)
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
      (cols: @unchecked) match {
        case (c1: BoolColumn, c2: BoolColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow) 
            if (thisVal == c2(thatRow)) EQ else if (thisVal) GT else LT
          }
        }

        case (c1: LongColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: LongColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: LongColumn, c2: NumColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: DoubleColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: DoubleColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: DoubleColumn, c2: NumColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = BigDecimal(c1(thisRow))
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: NumColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = BigDecimal(c2(thatRow))
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: NumColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = BigDecimal(c2(thatRow))
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: NumColumn, c2: NumColumn) => new RowComparator {
          val ord = Order[BigDecimal]
          def compare(thisRow: Int, thatRow: Int) = {
            ord.order(c1(thisRow), c2(thatRow))
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

        case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = EQ
        }

        case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = EQ
        }

        case (c1: NullColumn, c2: NullColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = EQ
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
        } yield compare0(array1(i1), array2(i2))).toArray

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
    def pairColumns(l1: List[ColumnRef], l2: List[ColumnRef], comparators: List[RowComparator]): List[RowComparator] = (l1, l2) match {
      case (h1 :: t1, h2 :: t2) if h1.selector == h2.selector => {
        val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)
        val (l2Equal, l2Rest) = l2.partition(_.selector == h2.selector)

        pairColumns(l1Rest, l2Rest, genComparatorFor(l1Equal, l2Equal) :: comparators)
      }

      case (h1 :: t1, h2 :: t2) if h1.selector < h2.selector => {
        val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)

        pairColumns(l1Rest, l2, genComparatorFor(l1Equal, Nil) :: comparators)
      }

      case (h1 :: t1, h2 :: t2) if h1.selector > h2.selector => {
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
