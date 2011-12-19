package com.reportgrid.storage.leveldb

import comparators._
import Bijection._

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

object SimpleTest {
  def main (argv : Array[String]) {
    val c = new Column(new File("/tmp/test"), ColumnComparator.BigDecimal)

    c.insert(12364534l, new BigDecimal("1.445322").as[Array[Byte]].as[ByteBuffer])
  }
}

// vim: set ts=4 sw=4 et:
