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

    val queries = (
      //// mega-query, takes ~86s
      //"""
      //import std::math::floor
      //
      //historic := //summer_games/historic_medals
      //
      //histogram := solve 'year
      //  maleCount := count(historic.Gender
      //    where historic.Gender = "Men" & historic.Edition = 'year)
      //  femaleCount := count(historic.Gender
      //    where historic.Gender = "Women" & historic.Edition = 'year)
      //
      //  {year: 'year, ratio: floor(100 * maleCount / femaleCount)}
      //
      //histogram
      //""" ::

      // fast, small queries
      "count(//obnoxious)" ::
      //"min(//obnoxious.v)" :: "max(//obnoxious.v)" ::
      //"sum(//obnoxious.v)" :: "mean(//obnoxious.v)" ::
      //"geometricMean(//obnoxious.v)" :: "sumSq(//obnoxious.v)" ::
      //"variance(//obnoxious.v)" :: "stdDev(//obnoxious.v)" ::
      Nil
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
