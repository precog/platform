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
