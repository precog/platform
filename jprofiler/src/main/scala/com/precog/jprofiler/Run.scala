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
      "count(//clicks)" :: "count(//fs2)" :: "count(//obnoxious)" :: Nil
    )

    config.ingest match {
      case Nil =>
        println("starting jprofiler")
        new Suite("jprofiling")(queries).run(config)
        println("finishing jprofiler")

      case qs =>
        println("starting ingest")
        new Suite("jprofiling")(Nil).run(config)
        println("finishing ingest")
    }
  }
}
