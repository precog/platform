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

import com.precog.bytecode.RandomLibrary

import org.specs2.mutable._

class TypeInferencerSpec extends Specification with DAG with RandomLibrary with TypeInferencer {
  import instructions._
  import dag._
  "type inference" should {
    "rewrite loads such that they will restrict the columns loaded" in {
      val line = Line(0, "")

      val input = Join(line, Map2Match(Add),
        Join(line, Map2Cross(DerefObject), 
          dag.LoadLocal(line, Root(line, PushString("/clicks"))),
          Root(line, PushString("time"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
          Root(line, PushString("height"))))

      todo
    }
  }
}

// vim: set ts=4 sw=4 et:
