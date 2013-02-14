package com.precog.yggdrasil
package nihdb

import com.precog.util.IOUtils

import org.specs2.mutable.{After, Specification}

class CookStateLogSpecs extends Specification {
  trait LogState extends After {
    val workDir = IOUtils.createTmpDir("cookstatespecs").unsafePerformIO

    def after = {
      IOUtils.recursiveDelete(workDir).unsafePerformIO
    }
  }

  "CookStateLog" should {
    "Properly initialize" in new LogState {
      val txLog = new CookStateLog(workDir)

      txLog.currentBlockId mustEqual 0l
      txLog.pendingCookIds must beEmpty
    }

    "Lock its directory during operation" in new LogState {
      val txLog = new CookStateLog(workDir)

      (new CookStateLog(workDir)) must throwAn[Exception]
    }
  }
}
