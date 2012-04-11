package com.precog.yggdrasil

import com.precog.common.VectorCase
import scala.annotation.tailrec
import scalaz._
import scalaz.Scalaz._
import scalaz.Ordering._

trait Slice { source =>
  import Slice._

  def idCount: Int
  def size: Int
  def isEmpty: Boolean = size == 0

  def identities: Seq[F0[Identity]]
  def columns: Map[CMeta, F0[_]]

  def iterator: Iterator[RowState] = new Iterator[RowState] {
    private var row = 0
    def hasNext = row < source.size
    def next = new RowState {
      def idAt(i: Int) = identities(i)(row)
      def valueAt(meta: CMeta) = columns(meta)(row)
    }
  }

  def map(meta: CMeta, refId: Long)(f: F1[_, _]): Slice = new Slice {
    val idCount = source.idCount
    val size = source.size

    val identities = source.identities
    val columns = source.columns.get(meta) map { f0 =>
                    source.columns + (CMeta(CDyn(refId), f.returns) -> (f0 andThen f))
                  } getOrElse {
                    sys.error("No column found in table matching " + meta)
                  }
  }

  def map2(m1: CMeta, m2: CMeta, refId: Long)(f: F2[_, _, _]): Slice = new Slice {
    val idCount = source.idCount
    val size = source.size

    val identities = source.identities
    val columns = {
      val cfopt = for {
        c1 <- source.columns.get(m1)
        c2 <- source.columns.get(m2)
      } yield {
        val fl  = m1.ctype.cast2l(f)
        val flr = m2.ctype.cast2r(fl)
        flr(m1.ctype.cast0(c1), m2.ctype.cast0(c2))
      }

      cfopt map { cf => 
        source.columns + (CMeta(CDyn(refId), cf.returns) -> cf)
      } getOrElse {
        sys.error("No column(s) found in table matching " + m1 + " and/or " + m2)
      }
    }
  }

  def filter(fx: (CMeta, F1[_, Boolean])*): Slice = {
    assert(fx forall { case (m, f0) => columns contains m })
    new Slice {
      private lazy val retained: Vector[Int] = {
        val f0x = fx map { case (m, f0) => m.ctype.cast1(f0)(m.ctype.cast0(columns(m))) }
        @tailrec def check(i: Int, acc: Vector[Int]): Vector[Int] = {
          if (i < source.size) check(i + 1, if (f0x.forall(_(i))) acc :+ i else acc)
          else acc
        }

        check(0, Vector())
      }

      val idCount = source.idCount
      lazy val size = retained.size
      lazy val identities = source.identities map { _ remap retained }
      lazy val columns = source.columns mapValues { _ remap retained }
    }
  }

  def sortByIdentities(idx: VectorCase[Int]): Slice = {
    assert(idx.length <= source.idCount)
    new Slice {
      private val sortedIndices: Array[Int] = {
        import java.util.Arrays
        val arr = Array.range(0, source.size)
        val accessors = idx.map(source.identities).toArray
        val comparator = new IntOrder {
          def order(i1: Int, i2: Int) = {
            var i = 0
            var result: Ordering = EQ
            while (i < accessors.length && (result eq EQ)) {
              val f0 = accessors(i)
              result = longInstance.order(f0(i1), f0(i2))
              i += 1
            }
            result
          }
        }

        Slice.qsort(arr, comparator)
        arr
      }
      
      val idCount = source.idCount
      lazy val size = source.size
      lazy val identities = source.identities map { _ remap sortedIndices }
      lazy val columns = source.columns mapValues { _ remap sortedIndices }
    }
  }

  def sortByValues(meta: CMeta*): Slice = {
    assert(meta.length <= source.idCount)
    new Slice {
      private val sortedIndices: Array[Int] = {
        import java.util.Arrays
        val arr = Array.range(0, source.size)
        val accessors = meta.map(m => (m.ctype, source.columns(m))).toArray
        val comparator = new IntOrder {
          def order(i1: Int, i2: Int) = {
            var i = 0
            var result: Ordering = EQ
            while (i < accessors.length && (result eq EQ)) {
              val (ctype, f0) = accessors(i)
              result = ctype.order(ctype.cast0(f0)(i1), ctype.cast0(f0)(i2))
              i += 1
            }
            result
          }
        }

        Slice.qsort(arr, comparator)
        arr
      }
      
      val idCount = source.idCount
      lazy val size = source.size
      lazy val identities = source.identities map { _ remap sortedIndices }
      lazy val columns = source.columns mapValues { _ remap sortedIndices }
    }
  }
}

object Slice {
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

  @inline private def swap(xs: Array[Int], i: Int, j: Int) {
    val temp = xs(i);
    xs(i) = xs(j);
    xs(j) = temp;
  }
}

trait Table {
  protected val slices: Iterable[Slice]

  
}
