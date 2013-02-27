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

  def timeM[M[+_]: Monad, A](s: String)(ma: => M[A]): M[A] = {
    val t0 = System.nanoTime()
    ma map { a =>
      val t = System.nanoTime() - t0
      System.err.println("%s took %.2fms" format (s, t / m))
      a
    }
  }

  /**
   * Intended to be used like so:
   * 
   *   def mycode() {
   *     Timing.initialize("starting timer")
   *     val a = doSomething()
   *     Timing.checkpoint("do something")
   *     val b = somethingElseWithA(a)
   *     Timing.checkpoint("something else with a")
   *     val result = blah(a, b)
   *     Timing.checkpoint("final result")
   *     result
   *   }
   * 
   * This would print:
   * 
   *   starting timer
   *   do something took 0.82ms
   *   something else with a took 13.22ms
   *   final result took 12.95ms
   */

  @volatile
  private var t0: Long = 0L

  def initialize(s: String) {
    t0 = System.nanoTime()
    System.err.println(s)
  }
  
  def checkpoint(s: String) {
    val t1: Long = System.nanoTime()
    val t: Long = t1 - t0
    t0 = t1
    System.err.println("%s took %.2fms" format (s, t / m))
  }

}
