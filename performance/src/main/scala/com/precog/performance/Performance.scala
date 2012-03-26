package com.precog.performance

import annotation.tailrec
import java.io.{PrintStream, PrintWriter}

class Performance(
    warmupDefaults: BenchmarkParameters = Performance.warmupDefaults,
    benchmarkDefaults: BenchmarkParameters = Performance.benchmarkDefaults) {
  def benchmark[T](test: => T,
                   warmupParams: BenchmarkParameters = warmupDefaults,
                   benchmarkParams: BenchmarkParameters = benchmarkDefaults): BenchmarkResults[T] = {
    benchmarkOnly(test, warmupParams)
    benchmarkOnly(test, benchmarkParams)
  }

  def benchmarkOnly[T](test: => T, parameters: BenchmarkParameters = benchmarkDefaults): BenchmarkResults[T] = {

    @tailrec
    def benchmark[A](test: => A, result: BenchmarkResults[A]): BenchmarkResults[A] = {
      if (result.testRuns < parameters.testRuns) {
         val (t, r) = time(result.repCount, test)
         if(parameters.gcBetweenTests) {
           attemptGC
         }
         parameters.restBetweenTests foreach { Thread.sleep }
         benchmark(test, result.add(t, r))
      } else {
        result
      }
    }

    def attemptGC() = {
      import System.gc
      gc;gc;gc;gc;gc; 
      gc;gc;gc;gc;gc; 
      gc
    }
    
    def repeatsRequired(reps: Int = 1): Int = {
      val (t, _) = time(reps, test)
      if(t < parameters.runMillisGoal * 1000000L) {
        repeatsRequired(reps*2)
      } else {
        reps
      }
    }

    def noop = ()
    
    val baseline = benchmark(noop, BenchmarkResults(0, 10, 0, Vector.empty[Long], Vector.empty[Unit]))
    
    benchmark(test,  BenchmarkResults(0, repeatsRequired(), baseline.meanRepTime(), Vector.empty[Long], Vector.empty[T]))
  }

  def time[T](repeat: Int, f: => T): (Long,  T) = {
    def rep(r: Int = 0): T = {
      if(r < repeat-1) {
        f
        rep(r+1)
      } else {
        f
      }
    }
    val start = System.nanoTime()
    val r = rep()
    val t = System.nanoTime() - start
    (t,  r)
  }
}

object Performance {
  
  val warmupDefaults = BenchmarkParameters(10, 1000)
  val benchmarkDefaults = BenchmarkParameters(200, 1000)

  def apply(warmupDefaults: BenchmarkParameters = this.warmupDefaults,
            benchmarkDefaults: BenchmarkParameters = this.benchmarkDefaults) =
    new Performance(warmupDefaults, benchmarkDefaults)
}

case class BenchmarkResults[T](
    testRuns: Int, 
    repCount: Int, 
    baseline: Double,
    timings: Vector[Long],
    results: Vector[T] = Vector.empty[T]) {
  def add(timing: Long, result: T): BenchmarkResults[T] = BenchmarkResults(testRuns + 1, repCount, baseline, timings :+ timing, results :+ result)
  
  private val reportTemplate = """
Performance Measurement Results [%s]
====================================================
Results for %d test runs of %d reps each
Rep Time ms (min,mean,max):  %10.02f  %10.02f  %10.02f
Rep Time ms (90,95,99):      %10.02f  %10.02f  %10.02f
Reps per s  (min,mean,max):  %10.02f  %10.02f  %10.02f
Reps per s  (90,95,99):      %10.02f  %10.02f  %10.02f
Measurement overhead:        %10.02f%%
"""
 
  def report(out: PrintStream): Unit = report("test", out)

  def report(label: String, out: PrintStream) {
    val maxIndex = testRuns-1
    val mean = meanRepTime
    val p90 = ptile(0.9)
    val p95 = ptile(0.95)
    val p99 = ptile(0.99)

    val ts = timings.sorted.map(_ / (repCount * 1000000000.0))
    out.println(reportTemplate.format(
      label,
      testRuns,
      repCount,
      ts.head * 1000, mean * 1000, ts.last * 1000,
      ts(p90) * 1000, ts(p95) * 1000, ts(p99) * 1000,
      1/ts.last, 1/meanRepTime, 1/ts.head,
      1/ts(p90), 1/ts(p95), 1/ts(p99),
      baseline * 100.0 / mean
    ))
  }
  private def ptile(p: Double): Int = {
    (p * (testRuns-1)).toInt
  }
  def meanRepTime(): Double = {
    val denom = timings.length * repCount * 1000000000.0
    timings.foldLeft(0.0) {
      case (acc, el) => acc + (el/denom)
    }
  }
}

case class BenchmarkParameters(
  testRuns: Int,
  runMillisGoal: Long,
  restBetweenTests: Option[Long] = None,
  gcBetweenTests: Boolean = false
)
