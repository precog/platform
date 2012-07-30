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
