package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.table.cf
import scalaz._

trait StdLibEvaluatorStack[M[+_]] 
    extends EvaluatorModule[M]
    with StdLibModule[M] 
    with StdLibOpFinderModule[M] 
    with StdLibStaticInlinerModule[M] 
    with ReductionFinderModule[M]
    with PredicatePullupsModule[M] {

  trait Lib extends StdLib with StdLibOpFinder with StdLibStaticInliner with ReductionFinder with PredicatePullups
  object library extends Lib

  abstract class Evaluator[N[+_]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M) 
      extends EvaluatorLike[N](N0)(mn, nm)
      with StdLibOpFinder 
      with StdLibStaticInliner {

    val Exists = library.Exists
    val Forall = library.Forall
    def concatString(ctx: EvaluationContext) = library.Infix.concatString.f2(ctx)
    def coerceToDouble(ctx: EvaluationContext) = cf.util.CoerceToDouble
  }
}

// vim: set ts=4 sw=4 et:
