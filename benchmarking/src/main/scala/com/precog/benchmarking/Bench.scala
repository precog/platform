package com.precog.benchmarking

import com.google.common.io.Files
import org.apache.commons.io.FileUtils

import com.weiglewilczek.slf4s.Logging

import java.io.File

trait Bench extends Logging {
  val name: String
  def time[T](f: () => T): (T, Long) = {
    val start = System.currentTimeMillis
    val result = f()
    (result, System.currentTimeMillis - start)
  }

  def setup(): File = {
    Files.createTempDir()
  }

  def performWrites(baseDir: File, count: Long): Unit
  def performReads(baseDir: File, count: Long): Unit

  def teardown(baseDir: File) {
    FileUtils.deleteDirectory(baseDir)
  }

  def run(warmups: Int, runs: Int, elementCount: Long) = {
    val totalRuns = runs + 20 // We'll drop the highest and lowest 10 from the results
    (1 to warmups).foreach { idx => {
      val base = setup()
      try {
        logger.info("Warmup %d writes in %d ms".format(idx, time(() => performWrites(base, elementCount))._2))
        logger.info("Warmup %d reads in %d ms".format(idx, time(() => performReads(base, elementCount))._2))
      } catch {
        case t: Throwable => logger.error("Error during bench", t)
      }
      teardown(base)
    }}

    val resultTimes = (1 to totalRuns).map { idx => {
      val base = setup()
      val runTime = 
        try {
          val writeTime = time(() => performWrites(base, elementCount))._2
          logger.info("Run %d writes in %d ms".format(idx, writeTime))
          val readTime  = time(() => performReads(base, elementCount))._2
          logger.info("Run %d reads in %d ms".format(idx, readTime))
          (writeTime,readTime)
        } catch {
          case t: Throwable => logger.error("Error during bench", t); (0l,0l)
        }
      teardown(base)
      runTime
    }}

    val (writeTimes,readTimes) = resultTimes.unzip match {
      case (w, r) => (w.drop(10).dropRight(10), r.drop(10).dropRight(10))
    }

    logger.info(name + " average write time = " + (writeTimes.sum * 1.0 / runs))
    logger.info(name + " average read time  = " + (readTimes.sum * 1.0 / runs))

     (writeTimes,readTimes)                 
  }
}
