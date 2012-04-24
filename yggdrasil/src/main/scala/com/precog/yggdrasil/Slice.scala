package com.precog.yggdrasil

import com.precog.common.VectorCase
import scala.annotation.tailrec
import scalaz.{Identity => _, _}
import scalaz.Scalaz._
import scalaz.Ordering._

trait Slice { source =>
  import Slice._

  def identities: Seq[Column[Identity]]
  protected[yggdrasil] def columns: Map[VColumnRef[_], Column[_]]
  def column[@specialized(Boolean, Long, Double) A](ref: VColumnRef[A]): Option[Column[A]] = {
    columns.get(ref).map(_.asInstanceOf[Column[A]])
  }


  def idCount: Int
  def size: Int
  def isEmpty: Boolean = size == 0

  def compareIdentityPrefix(other: Slice, prefixLength: Int, srow: Int, orow: Int): Ordering = {
    var ii = 0
    var result: Ordering = EQ
    while (ii < prefixLength && (result eq EQ)) {
      val i1: Long = identities(ii)(srow)
      val i2: Long = other.identities(ii)(orow)
      if (i1 < i2) result = LT else if (i1 > i1) result = GT
      ii += 1
    }

    result
  }

  def map(oldId: VColumnId, newId: VColumnId)(f: F1[_, _]): Slice = new Slice {
    private val argRef = VColumnRef[f.accepts.CA](oldId, f.accepts)
    val idCount = source.idCount
    val size = source.size

    val identities = source.identities
    val columns = source.column(argRef) map { col =>
                    val ctype = col.returns
                    source.columns + (VColumnRef[f.returns.CA](newId, f.returns) -> (ctype.cast0(col) |> ctype.cast1(f)))
                  } getOrElse {
                    sys.error("No column found in table matching " + argRef)
                  }
  }

  def remap(pf: PartialFunction[Int, Int]) = new Slice {
    val idCount = source.idCount
    val size = source.size
    val identities = source.identities.map(_.remap(pf))
    val columns = source.columns.mapValues(_.remap(pf))
  }

  def split(idx: Int): (Slice, Slice) = (
    new Slice {
      val idCount = source.idCount
      val size = idx

      val identities = source.identities map {
        _ remap {
          case i if i < idx => i
        }
      }

      val columns = source.columns.mapValues {
        _ remap {
          case i if i < idx => i
        }
      }
    },
    new Slice {
      val idCount = source.idCount
      val size = source.size - idx
      val identities = source.identities map {
        _ remap {
          case i if i < size => i + idx
        }
      }

      val columns = source.columns.mapValues {
        _ remap {
          case i if i < size => i + idx
        }
      }
    }
  )
    

  def map2(id_1: VColumnId, id_2: VColumnId, newId: VColumnId)(f: F2[_, _, _]): Slice = new Slice {
    private val ref_1 = VColumnRef(id_1, f.accepts._1)
    private val ref_2 = VColumnRef(id_2, f.accepts._2)
    private val refResult = VColumnRef(newId, f.returns) 

    val idCount = source.idCount
    val size = source.size

    val identities = source.identities
    val columns = {
      val cfopt = for {
        c1 <- source.columns.get(ref_1)
        c2 <- source.columns.get(ref_2)
      } yield {
        (f.accepts, f.returns) match {
          case ((CBoolean, CBoolean), CBoolean) => f.asInstanceOf[F2[Boolean, Boolean, Boolean]](c1.asInstanceOf[Column[Boolean]], c2.asInstanceOf[Column[Boolean]])
          case ((CInt, CInt), CInt) => f.asInstanceOf[F2[Int, Int, Int]](c1.asInstanceOf[Column[Int]], c2.asInstanceOf[Column[Int]])
          case ((CLong, CLong), CLong) => f.asInstanceOf[F2[Long, Long, Long]](c1.asInstanceOf[Column[Long]], c2.asInstanceOf[Column[Long]])
          case ((CFloat, CFloat), CFloat) => f.asInstanceOf[F2[Float, Float, Float]](c1.asInstanceOf[Column[Float]], c2.asInstanceOf[Column[Float]])
          case ((CDouble, CDouble), CDouble) => f.asInstanceOf[F2[Double, Double, Double]](c1.asInstanceOf[Column[Double]], c2.asInstanceOf[Column[Double]])
          case _ => f.applyCast(c1, c2)
        }
      }

      cfopt map { cf => 
        source.columns + (refResult -> cf)
      } getOrElse {
        sys.error("No column(s) found in table matching " + ref_1 + " and/or " + ref_2)
      }
    }
  }

  def filter(fx: (VColumnId, F1[_, Boolean])*): Slice = {
    assert(fx forall { case (id, f1) => columns contains VColumnRef(id, f1.accepts) })
    new Slice {
      private lazy val retained: Vector[Int] = {
        val f1x = fx map { case (id, f1) => f1.applyCast(columns(VColumnRef(id, f1.accepts))) }

        @tailrec def check(i: Int, acc: Vector[Int]): Vector[Int] = {
          if (i < source.size) check(i + 1, if (f1x.forall(_(i))) acc :+ i else acc)
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

  def sortByValues(meta: VColumnRef[_]*): Slice = {
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
              val f0t = ctype.cast0(f0)
              result = ctype.order(f0t(i1), f0t(i2))
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

  def append(other: Slice): Slice = {
    assert(columns.keySet == other.columns.keySet && idCount == other.idCount) 
    new Slice {
      val idCount = source.idCount
      val size = source.size + other.size
      val identities = (source.identities zip other.identities) map {
        case (c1, c2) => new Column[Long] { 
          val returns = CLong
          def isDefinedAt(row: Int) = (row >= 0 && row < source.size) || (row - source.size >= 0 && row - source.size < other.size)
          def apply(row: Int) = if (row < source.size) c1(row) else c2(row - source.size)
        }
      }

      val columns = other.columns.foldLeft(source.columns) {
        case (acc, (cmeta, col)) => 
          val ctype = cmeta.ctype
          val c1 = ctype.cast0(acc(cmeta))
          val c2 = ctype.cast0(col)
          acc + (
            cmeta -> {
              new Column[ctype.CA] { 
                val returns: CType { type CA = ctype.CA } = ctype
                def isDefinedAt(row: Int) = (row >= 0 && row < source.size) || (row - source.size >= 0 && row - source.size < other.size)
                def apply(row: Int) = if (row < source.size) c1(row) else c2(row - source.size)
              }
            }
          )
      }
    }
  }
}

class ArraySlice(idsData: VectorCase[Array[Long]], data: Map[VColumnRef[_], Object /* Array[_] */]) extends Slice {
  assert(idsData.toList.sliding(2) forall { case x :: y :: Nil => x.length == y.length; case _ => true })
  val idCount = idsData.length
  val size = idsData.map(_.length).reduceLeft(_ min _)
  val identities = idsData map { Column.forArray(CLong, _) }
  val columns: Map[VColumnRef[_], Column[_]] = 
    data map { 
      case (m @ VColumnRef(_, ctype), arr) =>
        (ctype: CType) match {
          case CBoolean => m -> Column.forArray[Boolean](CBoolean, arr.asInstanceOf[Array[Boolean]]) 
          case CLong    => m -> Column.forArray[Long](CLong, arr.asInstanceOf[Array[Long]]) 
          case CDouble  => m -> Column.forArray[Double](CDouble, arr.asInstanceOf[Array[Double]]) 
          case _        => m -> Column.forArray[ctype.CA](ctype, arr.asInstanceOf[Array[ctype.CA]]) 
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
