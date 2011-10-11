package com.reportgrid.quirrel
package parser

import edu.uwm.cs.gll.Failure
import edu.uwm.cs.gll.LineStream

case class ParseException(failures: Set[Failure]) extends RuntimeException {
  private val Pattern = "  error:%%d: %s%n    %%s%n    %%s%n"
  
  def mkString = {
    val errors = for (Failure(msg, tail) <- failures) yield {
      val partial = Pattern format msg
      tail formatError partial
    }
    
    errors.mkString("\n")
  }
}
