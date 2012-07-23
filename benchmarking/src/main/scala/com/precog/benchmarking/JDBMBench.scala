package com.precog.benchmarking

import jdbm._

import com.weiglewilczek.slf4s.Logging

import java.io.File
import java.util.SortedMap

import scala.collection.JavaConverters._

object JDBMBench extends Bench with Logging {
  val name = "JDBMBench"

  val mapName = "indexMap"

  def performWrites(baseDir: File, elementCount: Long) {
    var current = 0l
    val dbSys = RecordManagerFactory.createRecordManager(baseDir.getCanonicalPath + "/indexFile")
    
    val db: SortedMap[java.lang.Long,Long] = dbSys.treeMap(mapName)

    while (current < elementCount) {
      db.put(current, current)
      if (current % 200000 == 0) {
        dbSys.commit() // Prevent heap overrun
      }
      current += 1
    }

    dbSys.commit()
    dbSys.close()
  }
    
  def performReads(baseDir: File, elementCount: Long) {
    val dbSysRead = RecordManagerFactory.createRecordManager(baseDir.getCanonicalPath + "/indexFile")
    val dbRead: SortedMap[java.lang.Long,Long] = dbSysRead.treeMap(mapName)

    if (!dbRead.entrySet().iterator().asScala.forall { kvPair => kvPair.getKey() == kvPair.getValue() }) {
      logger.error("Mismatch on retrievals!")
    }

    dbSysRead.close()
  }
}
