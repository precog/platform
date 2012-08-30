package com.precog.yggdrasil.util

import com.precog.yggdrasil.{ IdSource, TableModule, YggConfigComponent }
import com.precog.yggdrasil.table.{ ArrayLongColumn, Column, CScanner }

import scala.collection.immutable.BitSet

trait IdSourceConfig {
  def idSource: IdSource
}

trait IdSourceScannerModule[M[+_]] extends TableModule[M] with YggConfigComponent {
  type YggConfig <: IdSourceConfig
  
  def freshIdScanner = new CScanner {
    type A = Unit
    def init = ()
    
    def scan(a: Unit, col: Set[Column], range: Range): (A, Set[Column]) = {
      val defined = BitSet(range filter { i => col exists { _ isDefinedAt i } }: _*)
      val values = range map { _ => yggConfig.idSource.nextId() } toArray
      
      ((), Set(ArrayLongColumn(defined, values)))
    }
  }
}
