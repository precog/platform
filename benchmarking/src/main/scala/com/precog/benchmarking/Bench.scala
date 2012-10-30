/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.benchmarking

import com.precog.util.IOUtils

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
    IOUtils.createTmpDir("benchmark").unsafePerformIO
  }

  def performWrites(baseDir: File, count: Long): Unit
  def performReads(baseDir: File, count: Long): Unit

  def teardown(baseDir: File) {
    IOUtils.recursiveDelete(baseDir).unsafePerformIO
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
