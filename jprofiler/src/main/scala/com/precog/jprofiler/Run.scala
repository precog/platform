package com.precog.jprofiler

import com.precog.ragnarok._

class Suite(name: String)(qs: List[String]) extends PerfTestSuite {
  override def suiteName = name
  qs.foreach(q => query(q))
}

object Run {
  def main(args: Array[String]): Unit = {
    val config = RunConfig.fromCommandLine(args.toList) | sys.error("invalid arguments!")
    val queries = (
      "count(//obnoxious)" ::
      "min(//obnoxious.v)" :: "max(//obnoxious.v)" ::
      "sum(//obnoxious.v)" :: "mean(//obnoxious.v)" ::
      "geometricMean(//obnoxious.v)" :: "sumSq(//obnoxious.v)" ::
      "variance(//obnoxious.v)" :: "stdDev(//obnoxious.v)" :: Nil
    )

    println("starting benchmark")
    new Suite("jprofiling")(queries).run(config)
    println("finishing benchmark")
  }
}
