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
package com.precog.bytecode

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.scalacheck.Prop
import java.nio.ByteBuffer

class BytecodeSpecs extends Specification
    with ScalaCheck
    with InstructionGenerators
    with UtilGenerators
    with BytecodeReader
    with BytecodeWriter {
  
  import Prop._

  object library extends RandomLibrary
  
  val numCores = Runtime.getRuntime.availableProcessors
  implicit val params = set(minTestsOk -> (10000 * numCores), workers -> numCores)
  
  "bytecode reader/writer" should {
    "be consistent" in check { stream: Vector[Instruction] =>
      val buffer = ByteBuffer.allocate(estimateSpace(stream))
      
      write(stream, buffer)
      buffer.rewind()
      val stream2 = read(buffer)
      
      stream2 mustEqual stream
    }
  }
}
