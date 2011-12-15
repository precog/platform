package com.querio.bytecode

import java.nio.ByteBuffer
import scala.annotation.tailrec

trait Writer extends Instructions {
  import instructions._
  
  def write(stream: Vector[Instruction], buffer: ByteBuffer)
}

trait BytecodeWriter extends Writer with Version {
  import instructions._
  
  def write(stream: Vector[Instruction], buffer: ByteBuffer) {
    val version = ((Major & 0x7F) << 24) & ((Minor & 0xFF) << 16) & (Release & 0xFFFF)
    writeInt(version, buffer)
    
    val table = createTable(Set(stream: _*))
    writeTable(table, buffer)
    writeInstructions(stream, table, buffer)
  }
  
  private def writeTable(table: Map[DataInstr, Int], buffer: ByteBuffer) {
    for ((instr, id) <- table) {
      buffer.putInt(id)
      
      instr match {
        case FilterMatch(_, pred) => writePredicate(pred, buffer)
        case FilterCross(_, pred) => writePredicate(pred, buffer)
        
        case Swap(depth) => {
          writeInt(4, buffer)
          writeInt(depth, buffer)
        }
        
        case Line(num, text) => writeLineInfo(num, text, buffer)
        
        case PushString(str) => writeString(str, buffer)
        case PushNum(num) => writeNum(num, buffer)
      }
    }
    
    buffer.putInt(0x0)
  }
  
  @tailrec
  private[this] def writeInstructions(stream: Vector[Instruction], table: Map[DataInstr, Int], buffer: ByteBuffer) {
    def unaryOpNum(op: UnaryOperation) = op match {
      case Comp => 0x40
      case Neg => 0x41
      
      case WrapArray => 0x61
    }
    
    def binaryOpNum(op: BinaryOperation) = op match {
      case Add => 0x00
      case Sub => 0x01
      case Mul => 0x02
      case Div => 0x03
      
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
      
      case DerefObject => 0xA0
      case DerefArray => 0xA1
    }
    
    def reductionNum(red: Reduction) = red match {
      case Count => 0x00
      
      case Mean => 0x01
      case Median => 0x02
      case Mode => 0x03
      
      case Max => 0x04
      case Min => 0x05
      
      case StdDev => 0x06
      case Sum => 0x07
    }
    
    def typeNum(tpe: Type) = tpe match {
      case Het => 0x00
    }
    
    if (!stream.isEmpty) {
      val (opcode, pad, arg) = stream.head match {
        case Map1(op) => (0x00, 0, unaryOpNum(op))
        case Map2Match(op) => (0x01, 0, binaryOpNum(op))
        case Map2CrossLeft(op) => (0x02, 0, binaryOpNum(op))
        case Map2CrossRight(op) => (0x04, 0, binaryOpNum(op))
        case Map2Cross(op) => (0x06, 0, binaryOpNum(op))
        
        case Reduce(red) => (0x08, 0, reductionNum(red))
        
        case VUnion => (0x10, 0, 0)
        case VIntersect => (0x11, 0, 0)
        
        case IUnion => (0x12, 0, 0)
        case IIntersect => (0x13, 0, 0)
        
        case i @ FilterMatch(depth, _) => (0x14, depth, table(i))
        case i @ FilterCross(depth, _) => (0x16, depth, table(i))
        
        case Split => (0x1A, 0, 0)
        case Merge => (0x1B, 0, 0)
        
        case Dup => (0x20, 0, 0)
        case i @ Swap(_) => (0x20, 0, table(i))
        
        case i @ Line(_, _) => (0x2A, 0, table(i))
        
        case LoadLocal(tpe) => (0x40, 0, typeNum(tpe))
        
        case i @ PushString(_) => (0x80, 0, table(i))
        case i @ PushNum(_) => (0x81, 0, table(i))
        case PushTrue => (0x82, 0, 0)
        case PushFalse => (0x83, 0, 0)
        case PushObject => (0x84, 0, 0)
        case PushArray => (0x85, 0, 0)
      }
      
      buffer.put(opcode.toByte)
      
      buffer.put(((pad >> 16) & 0xFF).toByte)
      buffer.put(((pad >> 8) & 0xFF).toByte)
      buffer.put((pad & 0xFF).toByte)
      
      buffer.putInt(arg)
      
      writeInstructions(stream.tail, table, buffer)
    }
  }
  
  private def writePredicate(pred: Predicate, buffer: ByteBuffer) {
    writeInt(pred.length, buffer)
    writePredicateStream(pred, buffer)
  }
  
  private[this] def writePredicateStream(stream: Vector[PredicateInstr], buffer: ByteBuffer) {
    if (!stream.isEmpty) {
      val opcode = stream.head match {
        case Add => 0x00
        case Sub => 0x01
        case Mul => 0x02
        case Div => 0x03
        
        case Neg => 0x41
        
        case Or => 0x30
        case And => 0x31
        
        case Comp => 0x40
        
        case DerefObject => 0xA0
        case DerefArray => 0xA1
        
        case Range => 0xFF
      }
      
      buffer.put(opcode.toByte)
      writePredicateStream(stream.tail, buffer)
    }
  }
  
  private def writeLineInfo(num: Int, text: String, buffer: ByteBuffer) {
    writeInt(text.length * 2 + 4, buffer)
    buffer.putInt(num)
    text foreach buffer.putChar
  }
  
  private def writeString(str: String, buffer: ByteBuffer) {
    writeInt(str.length * 2, buffer)
    str foreach buffer.putChar
  }
  
  private def writeNum(num: String, buffer: ByteBuffer) {
    writeString(num, buffer)
  }
  
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
      case i: FilterMatch => i -> currentInt()
      case i: FilterCross => i -> currentInt()
      
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
  
  def write(stream: Vector[Instruction], buffer: ByteBuffer) {
    // TODO
  }
}
