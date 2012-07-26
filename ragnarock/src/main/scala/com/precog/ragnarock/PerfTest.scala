package com.precog.ragnarock

import scalaz._


sealed trait PerfTest
case class RunQuery(query: String) extends PerfTest
case object RunSequential extends PerfTest
case object RunConcurrent extends PerfTest
case class Group(name: String) extends PerfTest


