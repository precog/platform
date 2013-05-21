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
package util

import com.precog.common._
import com.precog.yggdrasil.test._
import com.precog.yggdrasil.table._


import org.specs2.mutable._

trait IdSourceScannerModuleSpec[M[+_]] extends IdSourceScannerModule with TableModuleTestSupport[M] with Specification {
  private val blockSize = 10000

  private val cols = Map(ColumnRef(CPath.Identity, CBoolean) -> Column.const(true))

  private def scan(n: Int, sliceSize: Int)(scanner: CScanner): List[LongColumn] = {
    val (idCols, _) = (0 until n).foldLeft((List.empty[LongColumn], scanner.init)) { case ((idCols, acc0), i) =>
      val (acc, replCols) = scanner.scan(acc0, cols, (i * sliceSize) until ((i + 1) * sliceSize))
      val (col: LongColumn) :: Nil = replCols.values.toList
      (col :: idCols, acc)
    }
    idCols.reverse
  }

  "IdSourceScannerModule" should {
    "assign unique IDs" in {
      val idCols = scan(5, blockSize)(freshIdScanner)
      val ids = idCols.toSet flatMap { col: LongColumn =>
        (0 until blockSize).map(col(_)).toSet
      }
      ids must haveSize(5 * blockSize)
    }

    "be restartable across blockSize boundaries" in {
      val scanner = freshIdScanner
      val idCols0 = scan(5, blockSize)(scanner)
      val idCols1 = scan(5, blockSize)(scanner)

      (idCols0 zip idCols1) map { case (idCol0, idCol1) =>
        (0 until blockSize) foreach { row =>
          idCol0(row) must_== idCol1(row)
        }
      }
      ok
    }
  }
}



