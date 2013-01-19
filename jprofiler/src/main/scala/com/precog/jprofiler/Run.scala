package com.precog.jprofiler

import com.precog.ragnarok._

class Suite(name: String)(qs: List[String]) extends PerfTestSuite {
  override def suiteName = name
  qs.foreach(q => query(q))
}

object Run {
  def main(args: Array[String]): Unit = {
    val cwd = new java.io.File(".").getCanonicalFile
    val db = cwd.getName match {
      case "jprofiler" => "jprofiler.db"
      case _ => "jprofiler/jprofiler.db"
    }

    val args2 = args.toList ++ List("--root-dir", db)
    val config = RunConfig.fromCommandLine(args2) | sys.error("invalid arguments!")

    val queries = List(
      """
      conversions := //conversions
      customers := conversions.customer
      solve 'customer
        customers' := customers where customers.ID = 'customer
        distinct(customers' where customers'.age = max(customers'.age))
      """
    )

    config.rootDir match {
      case Some(d) if d.exists =>
        println("starting benchmark")
        new Suite("jprofiling")(queries).run(config)
        println("finishing benchmark")

      case Some(d) =>
        println("ERROR: --root-dir %s not found!" format d)
        println("did you forget to run 'extract-data'?")

      case None =>
        println("ERROR: --root-dir is missing somehow")
        println("default should have been %s" format db)
    }
  }
}
