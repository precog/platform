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
}
