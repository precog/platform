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
package com.precog.yggdrasil.util

import scala.annotation.tailrec

import com.precog.common._
import com.precog.yggdrasil.{ IdSource, TableModule, YggConfigComponent }
import com.precog.yggdrasil.table._

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import scalaz._

trait IdSourceConfig {
  def idSource: IdSource
}

trait IdSourceScannerModule extends YggConfigComponent {
  type YggConfig <: IdSourceConfig

  def freshIdScanner = new CScanner {
    type A = Long
    def init = 0
    private val id = yggConfig.idSource.nextId()

    def scan(pos: Long, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
      val rawCols = cols.values.toArray
      val defined = BitSetUtil.filteredRange(range.start, range.end) {
        i => Column.isDefinedAt(rawCols, i)
      }

      val idCol = new LongColumn {
        def isDefinedAt(row: Int) = defined(row)
        def apply(row: Int) = id
      }

      val seqCol = new LongColumn {
        def isDefinedAt(row: Int) = defined(row)
        def apply(row: Int): Long = pos + row
      }

      (pos + range.end, Map(
        ColumnRef(CPath(CPathIndex(0)), CLong) -> seqCol,
        ColumnRef(CPath(CPathIndex(1)), CLong) -> idCol))
    }
  }
}
