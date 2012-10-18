package com.precog.yggdrasil.util

import com.precog.common.json.CPath
import com.precog.yggdrasil.{ IdSource, TableModule, YggConfigComponent, CLong }
import com.precog.yggdrasil.table._

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

trait IdSourceConfig {
  def idSource: IdSource
}

trait IdSourceScannerModule[M[+_]] extends TableModule[M] with YggConfigComponent {
  type YggConfig <: IdSourceConfig
  
  def freshIdScanner = new CScanner {
    type A = Unit
    def init = ()
    
    def scan(a: Unit, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
      val rawCols = cols.values.toArray
      val defined = BitSetUtil.filteredRange(range.start, range.end) {
        i => Column.isDefinedAt(rawCols, i)
      }
      val values = new Array[Long](range.size)
      Loop.range(range.start, range.end) {
        i => values(i) = yggConfig.idSource.nextId()
      }
      
      ((), Map(ColumnRef(CPath.Identity, CLong) -> ArrayLongColumn(defined, values)))
    }
  }
}
