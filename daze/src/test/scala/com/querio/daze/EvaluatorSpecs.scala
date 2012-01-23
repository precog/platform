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
package com.querio
package daze

import org.specs2.mutable._

object EvaluatorSpecs extends Specification with Evaluator {
  import IterV._
  
  import dag._
  import instructions._
  
  "bytecode evaluator" should {
    "evaluate simple two-value multiplication" in {
      val line = Line(0, "")
      val input = Join(line, Map2Cross(Mul), Root(line, PushNum("6")), Root(line, PushNum("7")))
      val result = consumeEval(input)
      
      result must haveSize(1)
      
      val (_, sv) = result.head
      sv must beLike {
        case SDecimal(d) => d mustEqual 42
      }
    }
  }
  
  private def consumeEval(graph: DepGraph): Vector[SEvent] =
    (consume >>== eval(graph).enum) run { err => sys.error("O NOES!!!") }
  
  // apparently, this doesn't *really* exist in Scalaz
  private def consume: IterV[A, Vector[A]] = {
    def step(acc: Vector[A])(in: Input[A]): IterV[A, Vector[A]] = {
      in(el = { e => Cont(step(acc :+ e)) },
         empty = Cont(step(acc)),
         eof = Done(acc, EOF.apply))
    }
    
    Cont(step(Vector()))
  }
}
