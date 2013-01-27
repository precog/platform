package com.precog
package muspelheim 

import yggdrasil._
import yggdrasil.table.cf
import daze._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import scalaz._

trait ParseEvalStack[M[+_]] extends Compiler
    with LineErrors
    with ProvenanceChecker
    with Emitter 
    with EvaluatorModule[M] 
    with StdLibModule[M] 
    with StdLibOpFinderModule[M] 
    with StdLibStaticInlinerModule[M] {

  trait Lib extends StdLib with StdLibOpFinder 
  object library extends Lib

  abstract class Evaluator(M: Monad[M]) extends EvaluatorLike(M) with StdLibOpFinder with StdLibStaticInliner {
    val Exists = library.Exists
    val Forall = library.Forall
    def concatString(ctx: EvaluationContext) = library.Infix.concatString.f2(ctx)
    def coerceToDouble(ctx: EvaluationContext) = cf.util.CoerceToDouble
  }
}

