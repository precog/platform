package com.precog.yggdrasil
package util

import com.precog.common._
import com.precog.yggdrasil.test._
import com.precog.yggdrasil.table._

import org.specs2.mutable._

import scalaz._

trait IdSourceScannerModuleSpec[M[+_]] extends IdSourceScannerModule[M] with TableModuleTestSupport[M] with Specification {
  private val blockSize = 10000
  
  implicit def M: Monad[M] with Comonad[M]

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



