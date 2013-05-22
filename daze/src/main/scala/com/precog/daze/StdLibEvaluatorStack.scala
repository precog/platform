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
    with JoinOptimizerModule[M]
    with PredicatePullupsModule[M] {

  trait Lib extends StdLib with StdLibOpFinder
  object library extends Lib

  abstract class Evaluator[N[+_]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M) 
      extends EvaluatorLike[N](N0)(mn, nm)
      with StdLibOpFinder 
      with StdLibStaticInliner {

    val Exists = library.Exists
    val Forall = library.Forall
    def concatString(ctx: MorphContext) = library.Infix.concatString.f2(ctx)
    def coerceToDouble(ctx: MorphContext) = cf.util.CoerceToDouble
  }
}
