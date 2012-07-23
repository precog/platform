package com.precog.benchmarking

import org.xtreemfs.babudb._
import org.xtreemfs.babudb.api._
import org.xtreemfs.babudb.config._

import java.io.File
import java.nio.ByteBuffer
import java.util.Arrays

import scala.collection.JavaConverters._

object BabuDBBench extends Bench {
  val name = "BabuDBBench"

  def performWrites(baseDir: File, elementCount: Long) {
    var current = 0l
    val bytes = new Array[Byte](8)
    val buffer = ByteBuffer.wrap(bytes)

    val dbSys = BabuDBFactory.createBabuDB(new ConfigBuilder().setDataPath(baseDir.getCanonicalPath()).setMultiThreaded(4).build())
    val db = dbSys.getDatabaseManager().createDatabase("indexDB", 1)

    while (current < elementCount) {
      buffer.clear()
      buffer.putLong(current)

      db.singleInsert(0, bytes, bytes, null)
      current += 1
    }

    db.shutdown()
    dbSys.shutdown()
  }

  def performReads(baseDir: File, elementCount: Long) {
    val bytes = new Array[Byte](8)
    val buffer = ByteBuffer.wrap(bytes)

    val dbSysRead = BabuDBFactory.createBabuDB(new ConfigBuilder().setDataPath(baseDir.getCanonicalPath()).setMultiThreaded(4).build())
    val dbRead = dbSysRead.getDatabaseManager().getDatabase("indexDB")

    val firstIdx = { buffer.clear(); buffer.putLong(0l); bytes }
    val endIdx   = { buffer.clear(); buffer.putLong(Long.MaxValue); bytes }
    val result = dbRead.rangeLookup(0, firstIdx, endIdx, null)

    val iter = result.get()
    iter.asScala.forall { kvPair => Arrays.equals(kvPair.getKey(), kvPair.getValue()) }

    iter.free()

    dbRead.shutdown()
    dbSysRead.shutdown()
  }    
}
