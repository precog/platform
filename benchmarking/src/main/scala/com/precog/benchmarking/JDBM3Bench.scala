package com.precog.benchmarking

import org.apache.jdbm._

import com.weiglewilczek.slf4s.Logging

import java.io.File
import java.util.SortedMap

import scala.collection.JavaConverters._

object JDBM3Bench extends Bench with Logging {
  val name = "JDBM3Bench"

  val mapName = "indexMap"

  def performWrites(baseDir: File, elementCount: Long) {
    var current = 0l
    val dbSys = DBMaker.openFile(baseDir.getCanonicalPath + "/indexFile").make()
    
    val db: SortedMap[java.lang.Long,Long] = dbSys.createTreeMap(mapName)

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
    val dbSysRead = DBMaker.openFile(baseDir.getCanonicalPath + "/indexFile").make()
    val dbRead: SortedMap[java.lang.Long,Long] = dbSysRead.getTreeMap(mapName)

    val (allEqual,count) = dbRead.entrySet().iterator().asScala.foldLeft((true,0l)) { case ((allEqual,count),kvPair) => (allEqual && kvPair.getKey() == kvPair.getValue(),count + 1) }

    if (!allEqual) {
      logger.error("Mismatch on retrievals!")
    }

    if (count != elementCount) {
      logger.error("Incorrect count on retrievals")
    }

    logger.info("Results = " + (allEqual,count))

    dbSysRead.close()
  }
}
