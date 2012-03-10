package com.precog.performance

import com.precog.common.util.IOUtils

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.specs2.execute._

import java.io.File

trait PerformanceSpec extends ExamplesFactory {

  def perform(iterations: Int, time: Long)(test: => Any): Result = {
    def batchTest(iterations: Int) = {
      var cnt = 0
      while(cnt < iterations) {
        test 
        cnt += 1
      }
    }
    performBatch(iterations, time)(batchTest _)
  }

  def performBatch(iterations: Int, time: Long)(batchTest: Int => Any): Result = {
    test("warmup", batchTest(iterations))
    val testTime = test("measure", batchTest(iterations))
    val millis = testTime / 1000000
    if(millis <= time) {
      new Success("Nailed it! %.02f%% of %d".format( millis * 100.0 / time, time))
    } else {
      new Failure("Wiff! %.02f times goal of %d".format( millis.toDouble / time, time))
    }
  }

  def test(msg: String, test: => Any): Long = {
    val start = System.nanoTime
    test
    System.nanoTime - start
  }

  def newTempDir(): File = IOUtils.createTmpDir("preformance_test")

  def cleanupTempDir(dir: File) = IOUtils.recursiveDelete(dir)

}
