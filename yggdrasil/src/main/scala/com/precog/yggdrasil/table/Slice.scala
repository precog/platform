package com.precog.yggdrasil
package table

import com.precog.common.VectorCase

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList

import scala.annotation.tailrec
import scala.collection.breakOut
import scalaz.Ordering
import scalaz.Ordering._
import scalaz.ValidationNEL
import scalaz.Validation._
import scalaz.syntax.foldable._
import scalaz.syntax.semigroup._
import scalaz.std.iterable._

trait Slice { source =>
  import Slice._
  import Schema._

  def size: Int
  def isEmpty: Boolean = size == 0

  def columns: Map[ColumnRef, Column]

  lazy val valueColumns: Set[Column] = columns collect { case (ColumnRef(JPath.Identity, _), col) => col } toSet

  def mapColumns(f: CF1): Slice = new Slice {
    val size = source.size
    val columns = source.columns flatMap {
      case (ref, col) => f(col) map { (ref, _ ) }
    }
  }

  def deref(node : JPathNode) : Slice = new Slice {
    val size = source.size
    val columns = source.columns.collect {
      // case (ColumnRef(JPath(`node` :: rest), ctype), col) => (ColumnRef(JPath(rest), ctype), col) // TODO: why won't this work?
      case (ColumnRef(JPath(`node`, xs @ _*), ctype), col) => (ColumnRef(JPath(xs: _*), ctype), col)
    }
  }

  def wrap(wrapper : JPathNode) : Slice = new Slice {
    val size = source.size
    val columns = source.columns.map {
      case (ColumnRef(JPath(nodes @ _*), ctype), col) => (ColumnRef(JPath(wrapper +: nodes : _*), ctype), col)
    }
  }

  def typed(jtpe : JType) : Slice = new Slice {
    val size = source.size
    val columns = {
      if(subsumes(source.columns.map { case (ColumnRef(path, ctpe), _) => (path, ctpe) }(breakOut), jtpe))
        source.columns.filter { case (ColumnRef(path, ctpe), _) => includes(jtpe, path, ctpe) }
      else
        Map.empty[ColumnRef, Column]
    }
  }

  def nest(selectorPrefix: JPath) = new Slice {
    val size = source.size
    val columns = source.columns map { case (ColumnRef(selector, ctype), v) => ColumnRef(selectorPrefix \ selector, ctype) -> v }
  }

  def remap(pf: PartialFunction[Int, Int]) = new Slice {
    val size = source.size
    val columns: Map[ColumnRef, Column] = source.columns.mapValues(v => (v |> cf.util.Remap(pf)).get) //Remap is total
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

  def filter(fx: (JPath, Column => BoolColumn)*): Slice = {
    new Slice {
      lazy val filters = fx flatMap { 
        case (selector, f) => columns collect { case (ref, col) if ref.selector.hasPrefix(selector) => f(col) } 
      }

      lazy val retained: ArrayIntList = {
        @inline @tailrec def fill(i: Int, acc: ArrayIntList): ArrayIntList = {
          if (i < source.size && filters.forall(c => c.isDefinedAt(i) && c(i))) {
            fill(i + 1, acc)
          } else {
            acc
          }
        }

        fill(0, new ArrayIntList())
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

  def append(other: Slice): Slice = {
    new Slice {
      val size = source.size + other.size
      val columns = other.columns.foldLeft(source.columns) {
        case (acc, (ref, col)) => 
          acc + (ref -> acc.get(ref).flatMap(sc => cf.util.Concat(source.size)(sc, col)).getOrElse((col |> cf.util.Shift(source.size)).get))
      }
    }
  }

  def toJson(row: Int): JValue = {
    val steps = new scala.collection.mutable.ArrayBuffer[(ColumnRef, JValue, JValue)]()

    columns.foldLeft[JValue](JNothing) {
      case (jv, (ref @ ColumnRef(selector, _), col)) if col.isDefinedAt(row) => 
        steps += ((ref, jv, col.jValue(row)))
        try {
          jv.unsafeInsert(selector, col.jValue(row))
        } catch { 
          case ex => 
            steps.foreach(s => println(s + "\n\n"))
            ex.printStackTrace
            throw ex
        }

      case (jv, _) => jv
    }
  }

  def toValidatedJson(row: Int): ValidationNEL[Throwable, JValue] = {
    columns.foldLeft[ValidationNEL[Throwable, JValue]](success(JNull)) {
      case (jvv, (ref @ ColumnRef(selector, _), col)) if (col.isDefinedAt(row)) => 
        jvv flatMap { (_: JValue).insert(selector, col.jValue(row)).toValidationNel }

      case (jvv, _) => jvv
    }
  }

  def toString(row: Int): String = {
    (columns collect { case (ref, col) if col.isDefinedAt(row) => ref.toString + ": " + col.strValue(row) }).mkString("[", ", ", "]")
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
}
