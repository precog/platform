package com.reportgrid.storage
package leveldb

import Bijection._

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

object SimpleTest {
  def main (argv : Array[String]) {
    val c = LevelDBProjection(new File("/tmp/test"), Some(ProjectionComparator.BigDecimal)) ||| {
      errors => for (err <- errors.list) err.printStackTrace
                sys.error("Errors prevented creation of LevelDBProjection")
    }

    c.insert(12364534l, new BigDecimal("1.445322").as[Array[Byte]].as[ByteBuffer])
  }
}

// vim: set ts=4 sw=4 et:
