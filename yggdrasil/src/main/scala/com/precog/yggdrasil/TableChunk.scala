/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
