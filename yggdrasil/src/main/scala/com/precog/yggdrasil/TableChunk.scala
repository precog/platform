package com.precog.yggdrasil

import scala.annotation.tailrec
import scalaz._
import scalaz.Scalaz._
import scalaz.Ordering._

trait Tablet { source =>
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

  def map(meta: CMeta, refId: Long)(f: F1[_, _]): Tablet = new Tablet {
    val idCount = source.idCount
    val size = source.size

    val identities = source.identities
    val columns = source.columns.get(meta) map { f0 =>
                    source.columns + (CMeta(CDyn(refId), f.returns) -> (f0 andThen f))
                  } getOrElse {
                    sys.error("No column found in table matching " + meta)
                  }
  }

  def map2(m1: CMeta, m2: CMeta, refId: Long)(f: F2[_, _, _]): Tablet = new Tablet {
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

  def filter(fx: (CMeta, F1[_, Boolean])*): Tablet = {
    assert(fx forall { case (m, f0) => columns contains m })
    new Tablet {
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
}
