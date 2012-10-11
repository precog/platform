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
package com.precog
package yggdrasil
package table

import com.precog.util.NumericComparisons
import scalaz._
import scalaz.Ordering._

import scala.annotation.tailrec

trait RowComparator { self =>
  def compare(i1: Int, i2: Int): Ordering

  def swap: RowComparator = new RowComparator {
    def compare(i1: Int, i2: Int) = self.compare(i2, i1).complement
  }

  @tailrec
  final def nextLeftIndex(lmin: Int, lmax: Int, ridx: Int): Int = {
    compare(lmax, ridx) match {
      case LT => lmax + 1
      case GT => 
        if (lmax - lmin <= 1) {
          compare(lmin, ridx) match {
            case LT => lmax
            case GT | EQ => lmin
          }
        } else {
          val lmid = lmin + ((lmax - lmin) / 2)
          compare(lmid, ridx) match {
            case LT => nextLeftIndex(lmid + 1, lmax, ridx)
            case GT | EQ => nextLeftIndex(lmin, lmid - 1, ridx)
          }
        }
    
      case EQ => 
        if (lmax - lmin <= 1) {
          compare(lmin, ridx) match {
            case LT => lmax
            case GT | EQ => lmin
          }
        } else {
          val lmid = lmin + ((lmax - lmin) / 2)
          compare(lmid, ridx) match {
            case LT => nextLeftIndex(lmid + 1, lmax, ridx)
            case GT => sys.error("inputs on the left not sorted.")
            case EQ => nextLeftIndex(lmin, lmid - 1, ridx)
          }
        }
    }
  }
}

object RowComparator {
  def apply(col1: Column, col2: Column): RowComparator = {
    (col1, col2) match {
      case (c1: BoolColumn, c2: BoolColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          val thisVal = c1(thisRow) 
          if (thisVal == c2(thatRow)) EQ else if (thisVal) GT else LT
        }
      }

      case (c1: LongColumn, c2: LongColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: LongColumn, c2: DoubleColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: LongColumn, c2: NumColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: DoubleColumn, c2: LongColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: DoubleColumn, c2: DoubleColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: DoubleColumn, c2: NumColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: NumColumn, c2: LongColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: NumColumn, c2: DoubleColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: NumColumn, c2: NumColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          NumericComparisons.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: StrColumn, c2: StrColumn) => new RowComparator {
        val ord = Order[String]
        def compare(thisRow: Int, thatRow: Int) = {
          ord.order(c1(thisRow), c2(thatRow))
        }
      }

      case (c1: DateColumn, c2: DateColumn) => new RowComparator {
        def compare(thisRow: Int, thatRow: Int) = {
          val thisVal = c1(thisRow)
          val thatVal = c2(thatRow)
          if (thisVal isAfter thatVal) GT else if (thisVal == thatVal) EQ else LT
        }
      }

      case (c1, c2) => {
        val ordering = implicitly[Order[CType]].apply(c1.tpe, c2.tpe)
        
        // This also correctly catches CNullType cases.

        new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = ordering
        }
      }
    }
  }

  def apply(array1: Array[Column], array2: Array[Column]): RowComparator = {
    // Return the first column in the array defined at the row, or -1 if none are defined for that row
    @inline def firstDefinedIndexFor(columns: Array[Column], row: Int): Int = {
      var i = 0
      while (i < columns.length && ! columns(i).isDefinedAt(row)) { i += 1 }
      if (i == columns.length) -1 else i
    }

    new RowComparator {

      // Build an array of pairwise comparator functions for later use
      private val comparators: Array[RowComparator] = (for {
        i1 <- 0 until array1.length
        i2 <- 0 until array2.length
      } yield RowComparator(array1(i1), array2(i2)))(collection.breakOut)

      def compare(i: Int, j: Int) = {
        val first1 = firstDefinedIndexFor(array1, i)
        val first2 = firstDefinedIndexFor(array2, j)

        // In the following, undefined always sorts LT defined values
        if (first1 == -1 && first2 == -1) {
          EQ
        } else if (first1 == -1) {
          LT
        } else if (first2 == -1) {
          GT
        } else {
          // We have the indices, so use it to look up the comparator for the rows
          comparators(first1 * array2.length + first2).compare(i, j)
        }
      }
    }
  }
}
