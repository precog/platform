package com.precog.daze

import scalaz._

trait FullStdLibModule[M[+_]] extends StdLibOpFinderModule[M] 
    with ReductionFinderModule[M]
    with EvaluatorModule[M] {
  type Lib = StdLibOpFinder with StdLib with ReductionFinder
  val library = new StdLibOpFinder with StdLib with ReductionFinder {}
}
