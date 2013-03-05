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
