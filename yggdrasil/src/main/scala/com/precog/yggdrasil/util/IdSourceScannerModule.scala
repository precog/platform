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

trait IdSourceScannerModule[M[+_]] extends YggConfigComponent {
  type YggConfig <: IdSourceConfig
  
  implicit def M: Monad[M]
  
  // FIXME: This is less than ideal. Basically, we reserve IDs in blocks. The freshIdScanner
  // is mutable and remembers these reserved blocks. This let's freshIdScanners be restartable,
  // which should be a requirement as tables WILL be restarted. However, this is a heavy handed
  // solution to a simple problem. Ideally, we would use 2 ID columns. The first would be a
  // fixed Long for ALL rows. This Long would be obtained from an idSource. It would be tied
  // to the freshIdScanner and would never change for the life of the scanner. The 2nd column
  // would simply be the row #. These would be unique, easy to calculate, and not require
  // constantly access an AtomicLong for every row. More over, they would be trivially
  // restartable. This can't happen currently, because there are too many places in our code
  // that assume an ID columns have a specific shape (eg. exactly N columns). However, there
  // is a proposal to fix this. Once we have more flexible ID columns, we should revisit this.

  def freshIdScanner = new CScanner {
    private val blockSize: Int = 10000
    
    @volatile
    private var idBlocks: Array[Long] = new Array[Long](0)
    private val blockLock = new AnyRef

    @tailrec
    private final def fillArrayWithIds(ids: Array[Long], idsOffset: Int, from: Long) {
      val idx = (from / blockSize).toInt

      if (idx >= idBlocks.length) {
        blockLock synchronized {
          if (idx >= idBlocks.length) {
            val tmp = new Array[Long](idx + 1)
            System.arraycopy(idBlocks, 0, tmp, 0, idBlocks.length)
            tmp(idx) = yggConfig.idSource.nextIdBlock(blockSize)
            idBlocks = tmp
          }
        }
        fillArrayWithIds(ids, idsOffset, from)

      } else {
        val blockStart = idBlocks(idx)
        val blockEnd = blockStart + blockSize
        val innerOffset = from - idx * blockSize
        val initId = blockStart + innerOffset

        var i = idsOffset
        var id = initId
        while (id < blockEnd && i < ids.length) {
          ids(i) = id | Long.MinValue
          id += 1L
          i += 1
        }

        if (i < ids.length) {
          fillArrayWithIds(ids, i, from + (i - idsOffset))
        }
      }
    }

    type A = Long
    def init = 0
    def scan(pos: Long, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
      val rawCols = cols.values.toArray
      val defined = BitSetUtil.filteredRange(range.start, range.end) {
        i => Column.isDefinedAt(rawCols, i)
      }
      val values = new Array[Long](range.size)
      fillArrayWithIds(values, 0, pos)

      (pos + values.size, Map(ColumnRef(CPath.Identity, CLong) -> ArrayLongColumn(defined, values)))
    }
  }
}
