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
