/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

