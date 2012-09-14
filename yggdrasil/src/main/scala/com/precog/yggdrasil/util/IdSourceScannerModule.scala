package com.precog.yggdrasil.util

import com.precog.common.json.CPath
import com.precog.yggdrasil.{ IdSource, TableModule, YggConfigComponent, CLong }
import com.precog.yggdrasil.table._

import scala.collection.immutable.BitSet

trait IdSourceConfig {
  def idSource: IdSource
}

trait IdSourceScannerModule[M[+_]] extends TableModule[M] with YggConfigComponent {
  type YggConfig <: IdSourceConfig
  
  def freshIdScanner = new CScanner {
    type A = Unit
    def init = ()
    
    def scan(a: Unit, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
      val defined = BitSet(range filter { i => cols.exists(_._2.isDefinedAt(i)) }: _*)
      val values = range map { _ => yggConfig.idSource.nextId() } toArray
      
      ((), Map(ColumnRef(CPath.Identity, CLong) -> ArrayLongColumn(defined, values)))
    }
  }
}
