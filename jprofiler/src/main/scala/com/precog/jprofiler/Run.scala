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


    val queries = (
      /* "count(//obnoxious)" ::
      "min(//obnoxious.v)" :: "max(//obnoxious.v)" ::
      "sum(//obnoxious.v)" :: "mean(//obnoxious.v)" ::
      "geometricMean(//obnoxious.v)" :: "sumSq(//obnoxious.v)" ::
      "variance(//obnoxious.v)" :: "stdDev(//obnoxious.v)" */
      //"""
      //| medals := //summer_games/london_medals
      //| athletes := //summer_games/athletes
      //| 
      //| medals' := medals where medals.Age > 33
      //| athletes' := athletes where athletes.Countryname = "Tanzania"
      //| 
      //| medals' ~ athletes'
      //|   [medals', athletes']
      //| """.stripMargin :: Nil
      """
      athletes := //summer_games/athletes

      solve 'athlete
        athlete := athletes where athletes = 'athlete

        athlete' := {
          "Countryname": athlete.Countryname,
          "Population": athlete.Population,
          "Sex": athlete.Sex,
          "Sportname": athlete.Sportname,
          "Name": athlete.Name
        }

        { count: count(athlete), athlete: athlete' }
      """ :: Nil
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
