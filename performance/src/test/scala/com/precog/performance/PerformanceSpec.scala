package com.precog.performance

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.specs2.execute._

trait PerformanceSpec extends ExamplesFactory {

  implicit def stringToPerformance(label: String): PerformanceExample = new PerformanceExample(label)

  class PerformanceExample(label: String) {
    def perform(iterations: Int, time: Long)(test: => Any): Example = {
      def batchTest(iterations: Int) = {
        var cnt = 0
        while(cnt < iterations) {
          test 
          cnt += 1
        }
      }
      performBatch(iterations, time)(batchTest _)
    }

    def performBatch(iterations: Int, time: Long)(batchTest: Int => Any): Example = {
      val ex = Example(label, {
        test("warmup", batchTest(iterations))
        val testTime = test("measure", batchTest(iterations))
        val millis = testTime / 1000000
        if(millis <= time) {
          new Success("Nailed it! %.02f%% of %d".format( millis * 100.0 / time, time))
        } else {
          new Failure("Wiff! %.02f times goal of %d".format( millis.toDouble / time, time))
        }
      })
      exampleFactory.newExample(ex)
    }

    def test(msg: String, test: => Any): Long = {
      val start = System.nanoTime
      test
      System.nanoTime - start
    }
  } 
}
