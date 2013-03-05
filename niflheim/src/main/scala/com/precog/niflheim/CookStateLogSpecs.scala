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
package com.precog.niflheim

import com.precog.util.IOUtils

import java.util.concurrent.ScheduledThreadPoolExecutor

import org.specs2.mutable.{After, Specification}

class CookStateLogSpecs extends Specification {
  val txLogScheduler = new ScheduledThreadPoolExecutor(5)

  trait LogState extends After {
    val workDir = IOUtils.createTmpDir("cookstatespecs").unsafePerformIO

    def after = {
      IOUtils.recursiveDelete(workDir).unsafePerformIO
    }
  }

  "CookStateLog" should {
    "Properly initialize" in new LogState {
      val txLog = new CookStateLog(workDir, txLogScheduler)

      txLog.currentBlockId mustEqual 0l
      txLog.pendingCookIds must beEmpty
    }

    "Lock its directory during operation" in new LogState {
      val txLog = new CookStateLog(workDir, txLogScheduler)

      (new CookStateLog(workDir, txLogScheduler)) must throwAn[Exception]
    }
  }
}
