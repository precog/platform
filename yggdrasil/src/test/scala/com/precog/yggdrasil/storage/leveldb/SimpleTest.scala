package com.precog.storage
package leveldb

import com.precog.common.VectorCase
import com.precog.util.Bijection
import com.precog.yggdrasil.leveldb.LevelDBProjection

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

object SimpleTest {
  def main (argv : Array[String]) {
    val c = LevelDBProjection.forDescriptor(new File("/tmp/test"), sys.error("todo") /* Some(ProjectionComparator.BigDecimal)*/).unsafePerformIO

    c.insert(VectorCase(12364534l), sys.error("todo") /*new BigDecimal("1.445322").as[Array[Byte]].as[ByteBuffer]*/)
  }
}

// vim: set ts=4 sw=4 et:
