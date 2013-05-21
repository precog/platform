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

import java.nio.ByteBuffer
import scala.annotation.tailrec

trait Writer extends Instructions {
  import instructions._
  
  def estimateSpace(stream: Vector[Instruction]): Int
  def write(stream: Vector[Instruction], buffer: ByteBuffer): Int
  
  def writeAndTrim(stream: Vector[Instruction]): ByteBuffer = {
    val buffer = ByteBuffer.allocate(estimateSpace(stream))
    write(stream, buffer)
    buffer.flip()
    buffer
  }
}

trait BytecodeWriter extends Writer with Version {
  import instructions._
  
  def estimateSpace(stream: Vector[Instruction]) = {
    val dataEstimate = 200 * 8
    val tableEstimate = stream.length * (4 + 4 + dataEstimate) 
    (stream.length * 8) + 4 + 4 + tableEstimate
  }
  
  def write(stream: Vector[Instruction], buffer: ByteBuffer): Int = {
    val version = ((Major & 0x7F) << 24) | ((Minor & 0xFF) << 16) | (Release & 0xFFFF)
    writeInt(version, buffer)
    
    val table = createTable(Set(stream: _*))
    4 + writeTable(table, buffer) + writeInstructions(stream, table, buffer, 0)
  }
  
  private def writeTable(table: Map[DataInstr, Int], buffer: ByteBuffer) = {
    val lengths = for ((instr, id) <- table) yield {
      buffer.putInt(id)

      val pred = instr match {
        case FilterMatch => 0
        case FilterCross => 0
        case FilterCrossLeft => 0
        case FilterCrossRight => 0
        
        case Swap(depth) => {
          writeInt(4, buffer)
          writeInt(depth, buffer)
          8
        }

        case Line(num, col, text) => writeLineInfo(num, col, text, buffer)
        
        case PushString(str) => writeString(str, buffer)
        case PushNum(num) => writeNum(num, buffer)
      }

      4 + pred
    }
    
    buffer.putInt(0x0)
    
    lengths.sum + 4
  }
  
  @tailrec
  private[this] def writeInstructions(stream: Vector[Instruction], table: Map[DataInstr, Int], buffer: ByteBuffer, written: Int): Int = {
    def morph1Num(m1: BuiltInMorphism1) = m1 match {
      case BuiltInMorphism1(m) => 0xC1 | (m.opcode << 8)
    }

    def morph2Num(m2: BuiltInMorphism2) = m2 match {
      case BuiltInMorphism2(m) => 0xC2 | (m.opcode << 8)
    }

    def unaryOpNum(op: UnaryOperation) = op match {
      case Comp => 0x40
      case Neg => 0x41
      
      case New => 0x60
      
      case WrapArray => 0x61

      case BuiltInFunction1Op(op) => 0xB0 | (op.opcode << 8)
    }
    
    def binaryOpNum(op: BinaryOperation) = op match {
      case Add => 0x00
      case Sub => 0x01
      case Mul => 0x02
      case Div => 0x03
      case Mod => 0x04
      case Pow => 0x05
      
      case Lt => 0x10
      case LtEq => 0x11
      case Gt => 0x12
      case GtEq => 0x13
      
      case Eq => 0x20
      case NotEq => 0x21
      
      case Or => 0x30
      case And => 0x31
      
      case WrapObject => 0x60
      
      case JoinObject => 0x80
      case JoinArray => 0x81
      
      case ArraySwap => 0x8A
      
      case DerefObject => 0xA0
      case DerefMetadata => 0xA1
      case DerefArray => 0xA2

      case BuiltInFunction2Op(op) => 0xB1 | (op.opcode << 8)
    }
    
    def reductionNum(red: BuiltInReduction) = red match {
      case BuiltInReduction(red) => 0xC0 | (red.opcode << 8)
    }
    
    if (!stream.isEmpty) {
      val (opcode, pad, arg) = stream.head match {
        case Map1(op) => (0x00, 0.toShort, unaryOpNum(op))
        case Map2Match(op) => (0x01, 0.toShort, binaryOpNum(op))
        case Map2Cross(op) => (0x02, 0.toShort, binaryOpNum(op))
        case Map2CrossLeft(op) => (0x04, 0.toShort, binaryOpNum(op))
        case Map2CrossRight(op) => (0x06, 0.toShort, binaryOpNum(op))
        
        case Reduce(red) => (0x08, 0.toShort, reductionNum(red))
        case Morph1(m1) => (0x09, 0.toShort, morph1Num(m1))
        case Morph2(m2) => (0x19, 0.toShort, morph2Num(m2))
        
        case Assert => (0x10, 0.toShort, 0)
        
        case IUnion => (0x12, 0.toShort, 0)
        case IIntersect => (0x13, 0.toShort, 0)

        case SetDifference => (0x15, 0.toShort, 0)
        
        case FilterMatch => (0x14, 0.toShort, 0)
        case FilterCross => (0x16, 0.toShort, 0)
        case FilterCrossLeft => (0x17, 0.toShort, 0)
        case FilterCrossRight => (0x18, 0.toShort, 0)
        
        case Group(id) => (0x1A, 0.toShort, id)
        case MergeBuckets(and) => (0x1B, 0.toShort, if (and) 0x01 else 0x00)
        case KeyPart(id) => (0x1C, 0.toShort, id)
        case Extra => (0x1D, 0.toShort, 0)
        
        case Split => (0x1E, 0.toShort, 0)
        case Merge => (0x1F, 0.toShort, 0)

        case Distinct => (0x03, 0.toShort, 0)
        
        case Dup => (0x20, 0.toShort, 0)
        case Drop => (0x21, 0.toShort, 0)
        case i @ Swap(_) => (0x28, 0.toShort, table(i))
        
        case i @ Line(_, _, _) => (0x2A, 0.toShort, table(i))
        
        case LoadLocal => (0x40, 0.toShort, 0)
        
        case i @ PushString(_) => (0x80, 0.toShort, table(i))
        case i @ PushNum(_) => (0x81, 0.toShort, table(i))
        case PushTrue => (0x82, 0.toShort, 0)
        case PushFalse => (0x83, 0.toShort, 0)
        case PushObject => (0x84, 0.toShort, 0)
        case PushArray => (0x85, 0.toShort, 0)
        case PushNull => (0x86, 0.toShort, 0)
        case PushUndefined => (0x87, 0.toShort, 0)
        
        case PushGroup(id) => (0x90, 0.toShort, id)
        case PushKey(id) => (0x91, 0.toShort, id)
      }
      
      buffer.put(opcode.toByte)
      
      buffer.put(((pad >> 16) & 0xFF).toByte)
      buffer.put(((pad >> 8) & 0xFF).toByte)
      buffer.put((pad & 0xFF).toByte)
      
      buffer.putInt(arg)
      
      writeInstructions(stream.tail, table, buffer, written + 8)
    } else {
      written
    }
  }
  
  private def writeLineInfo(num: Int, col: Int, text: String, buffer: ByteBuffer) = {
    writeInt(text.length * 2 + 4 + 4, buffer)
    buffer.putInt(num)
    buffer.putInt(col)
    text foreach buffer.putChar
    4 + 4 + (text.length * 8)
  }
  
  private def writeString(str: String, buffer: ByteBuffer) = {
    writeInt(str.length * 2, buffer)
    str foreach buffer.putChar
    4 + (str.length * 8)
  }
  
  private def writeNum(num: String, buffer: ByteBuffer) =
    writeString(num, buffer)
  
  private def writeInt(len: Int, buffer: ByteBuffer) {
    buffer.putInt(len)
  }
  
  private def createTable(instructions: Set[Instruction]): Map[DataInstr, Int] = {
    var _currentInt = 0
    
    def currentInt() = {
      _currentInt += 1
      _currentInt
    }
    
    instructions.collect({
      case i: Swap => i -> currentInt()
      
      case i: Line => i -> currentInt()
      
      case i: PushString => i -> currentInt()
      case i: PushNum => i -> currentInt()
    })(collection.breakOut)
  }
}

/**
 * Writes a stream of instructions using the ASCII mnemonic format.
 */
trait MnemonicWriter extends Writer {
  import instructions._
  
  def estimateSpace(stream: Vector[Instruction]) = 0
  
  def write(stream: Vector[Instruction], buffer: ByteBuffer) = 0         // TODO
}
