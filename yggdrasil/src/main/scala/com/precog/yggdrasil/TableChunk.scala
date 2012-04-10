package com.precog.yggdrasil

import scalaz._
import scalaz.Scalaz._
import scalaz.Ordering._

trait Tablet { self =>
  def idCount: Int
  def size: Int

  def identities: Seq[F0[Identity]]
  def columns: Map[CMeta, F0[_]]

  def iterator: Iterator[RowState] = new Iterator[RowState] {
    private var row = 0
    def hasNext = row < self.size
    def next = new RowState {
      def idAt(i: Int) = identities(i)(row)
      def valueAt(meta: CMeta) = columns(meta)(row)
    }
  }

  def isEmpty: Boolean = size == 0

  def map(meta: CMeta, refId: Long)(f: F1[_, _]): Tablet = new Tablet {
    val idCount = self.idCount
    val size = self.size

    val identities = self.identities
    val columns = self.columns.get(meta) map { f0 =>
                    self.columns + (CMeta(CDyn(refId), f.returns) -> (f0 andThen f))
                  } getOrElse {
                    sys.error("No column found in table matching " + meta)
                  }
  }

  def map2(m1: CMeta, m2: CMeta, refId: Long)(f: F2[_, _, _]): Tablet = new Tablet {
    val idCount = self.idCount
    val size = self.size

    val identities = self.identities
    val columns = {
      val cfopt = for {
        c1 <- self.columns.get(m1)
        c2 <- self.columns.get(m2)
      } yield {
        val fl  = m1.ctype.cast2l(f)
        val flr = m2.ctype.cast2r(fl)
        flr(m1.ctype.cast0(c1), m2.ctype.cast0(c2))
      }

      cfopt map { cf => 
        self.columns + (CMeta(CDyn(refId), cf.returns) -> cf)
      } getOrElse {
        sys.error("No column(s) found in table matching " + m1 + " and/or " + m2)
      }
    }
  }

  def filter(fx: (CMeta, F1[_, Boolean])*): Tablet = sys.error("todo")
}
