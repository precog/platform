package com.precog.daze

import scala.collection.immutable.BitSet

import com.precog.yggdrasil.table._

trait IdSourceScannerModule[M[+_]] extends Evaluator[M] with ColumnarTableModule[M] {
  def freshIdScanner = new CScanner {
    type A = Unit
    def init = ()
    
    def scan(a: Unit, col: Column, range: Range): (A, Option[Column]) = {
      val defined = BitSet(range filter col.isDefinedAt: _*)
      val values = range map { _ => yggConfig.idSource.nextId() } toArray
      
      ((), Some(ArrayLongColumn(defined, values)))
    }
  }
}
