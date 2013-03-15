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
package com.precog.util

import scalaz._
import scalaz.syntax.monad._

object Timing {
  private final val m: Double = 1000000.0

  /**
   * Intended to be used like so:
   * 
   *   def mycode() {
   *     val a = foo()
   *     val b = bar()
   *     val c = Timing.time("crazy calculation") {
   *       crazyCalculation(a, b)
   *     }
   *     if (c > 0) c else a
   *   }
   * 
   * This would print:
   * 
   *   crazy calculation took 45.12ms
   */
  def time[A](s: String)(thunk: => A): A = {
    val t0 = System.nanoTime()
    val result = thunk
    val t = System.nanoTime() - t0
    System.err.println("%s took %.2fms" format (s, t / m))
    result
  }

  def time[A](f: A => String)(thunk: => A): A = {
    val t0 = System.nanoTime()
    val result = thunk
    val t = System.nanoTime() - t0
    System.err.println("%s took %.2fms" format (f(result), t / m))
    result
  }

  def timeM[M[+_]: Monad, A](s: String)(ma: => M[A]): M[A] = {
    val t0 = System.nanoTime()
    ma map { a =>
      val t = System.nanoTime() - t0
      System.err.println("%s took %.2fms" format (s, t / m))
      a
    }
  }

  def timeM[M[+_]: Monad, A](f: A => String)(ma: => M[A]): M[A] = {
    val t0 = System.nanoTime()
    ma map { a =>
      val t = System.nanoTime() - t0
      System.err.println("%s took %.2fms" format (f(a), t / m))
      a
    }
  }

  def timeStreamT[M[+_]: Monad, A](s: String)(stream: => StreamT[M, A]): StreamT[M, A] = {
    val t0 = System.nanoTime()
    val end = StreamT((StreamT.Skip {
      val t = System.nanoTime() - t0
      System.err.println("%s took %.2fms" format (s, t / m))
      StreamT.empty[M, A]
    }).point[M])
    stream ++ end
  }

  def timeStreamTElem[M[+_]: Monad, A](s: String)(stream0: => StreamT[M, A]): StreamT[M, A] = {
    def timeElem(stream: StreamT[M, A]): StreamT[M, A] = {
      val t0 = System.nanoTime()
      StreamT(stream.uncons map {
        case Some((a, tail)) =>
          val t = System.nanoTime() - t0
          System.err.println("%s took %.2fms" format (s, t / m))
          StreamT.Yield(a, timeElem(tail))

        case None =>
          StreamT.Done
      })
    }

    timeElem(stream0)
  }
}
