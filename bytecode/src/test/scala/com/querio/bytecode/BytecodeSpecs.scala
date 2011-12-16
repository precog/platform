package com.querio.bytecode

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
