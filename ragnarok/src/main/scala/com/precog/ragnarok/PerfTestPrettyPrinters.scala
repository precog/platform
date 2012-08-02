package com.precog.ragnarok

import scalaz._


final class PerfTestStatsPrettyPrinter(result: Tree[(PerfTest, Option[Statistics])]) {

  private def prettyStats(stats: Option[Statistics], toMillis: Double => Double): String =
    stats map { s => 
      "%.1f ms  (s = %.1f ms)" format (toMillis(s.mean), toMillis(s.stdDev))
    } getOrElse ""
    
  def prettyStats(toMillis: Double => Double = identity): String = {
    def lines(test: Tree[(PerfTest, Option[Statistics])]): List[String] = {
      test match {
        case Tree.Node((Group(name), _), kids) =>
          name :: (kids.toList flatMap (lines(_)))

        case Tree.Node((RunSequential, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" + " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) :+ (" ` " + prettyStats(s, toMillis))

        case Tree.Node((RunConcurrent, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" * " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) :+ (" ` " + prettyStats(s, toMillis))

        case Tree.Node((RunQuery(q), s), kids) =>
          ("-> " + q) :: ("   " + prettyStats(s, toMillis)) :: Nil
      }
    }

    lines(result) mkString "\n"
  }
}

object PerfTestPrettyPrinters {
  implicit def statsPrinter(result: Tree[(PerfTest, Option[Statistics])]) =
    new PerfTestStatsPrettyPrinter(result)
}



