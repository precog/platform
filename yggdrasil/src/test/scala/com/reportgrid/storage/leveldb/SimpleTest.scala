package com.reportgrid.storage
package leveldb

import com.reportgrid.util.Bijection
import com.reportgrid.yggdrasil.leveldb.ProjectionComparator
import com.reportgrid.yggdrasil.leveldb.LevelDBProjection

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

object SimpleTest {
  def main (argv : Array[String]) {
    val c = LevelDBProjection(new File("/tmp/test"), sys.error("todo") /* Some(ProjectionComparator.BigDecimal)*/) ||| {
      errors => for (err <- errors.list) err.printStackTrace
                sys.error("Errors prevented creation of LevelDBProjection")
    }

    c.insert(Vector(12364534l), sys.error("todo") /*new BigDecimal("1.445322").as[Array[Byte]].as[ByteBuffer]*/)
  }
}

// vim: set ts=4 sw=4 et:
