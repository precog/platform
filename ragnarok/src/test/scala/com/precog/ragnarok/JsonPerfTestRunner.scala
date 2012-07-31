package com.precog
package ragnarok

import scalaz._

import muspelheim.RawJsonColumnarTableStorageModule


final class JsonPerfTestRunner[M[+_], T](val timer: Timer[T], _optimize: Boolean, _userUID: String)
    (implicit val M: Monad[M]) extends EvaluatingPerfTestRunner[M, T]
    with RawJsonColumnarTableStorageModule[M] {
  type YggConfig = PerfTestRunnerConfig
  object yggConfig extends EvaluatingPerfTestRunnerConfig {
    val userUID = _userUID
    val optimize = _optimize
  }
}


