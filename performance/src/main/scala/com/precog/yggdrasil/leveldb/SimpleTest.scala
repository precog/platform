package com.precog.yggdrasil
package leveldb

import iterable.LevelDBProjectionFactory
import com.precog.common.util.IOUtils
import com.precog.common.VectorCase

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

import scalaz._
import scalaz.effect.IO 

object SimpleTest extends LevelDBProjectionFactory{
  def storageLocation(descriptor: ProjectionDescriptor): IO[File] = {
    IO { IOUtils.createTmpDir("columnSpec") }
  }

  def saveDescriptor(descriptor: ProjectionDescriptor): IO[Validation[Throwable, File]] = {
    storageLocation(descriptor) map { Success(_) }
  }

  def main (argv : Array[String]) {
    val c = projection(new File("/tmp/test"), sys.error("todo") /* Some(ProjectionComparator.BigDecimal)*/) ||| {
      errors => for (err <- errors.list) err.printStackTrace
                sys.error("Errors prevented creation of projection")
    }

    c.insert(VectorCase(12364534l), sys.error("todo") /*new BigDecimal("1.445322").as[Array[Byte]].as[ByteBuffer]*/)
  }
}

// vim: set ts=4 sw=4 et:
