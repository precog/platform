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

