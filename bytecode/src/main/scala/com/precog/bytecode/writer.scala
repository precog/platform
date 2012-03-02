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
        case FilterMatch(_, Some(pred)) => writePredicate(pred, buffer)
        case FilterCross(_, Some(pred)) => writePredicate(pred, buffer)
        case FilterCrossLeft(_, Some(pred)) => writePredicate(pred, buffer)
        case FilterCrossRight(_, Some(pred)) => writePredicate(pred, buffer)
        
        case FilterMatch(_, None) => 0
        case FilterCross(_, None) => 0
        
        case Swap(depth) => {
          writeInt(4, buffer)
          writeInt(depth, buffer)
          8
        }
        
        case Line(num, text) => writeLineInfo(num, text, buffer)
        
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
    def unaryOpNum(op: UnaryOperation) = op match {
      case Comp => 0x40
      case Neg => 0x41
      
      case New => 0x60
      
      case WrapArray => 0x61

      case BuiltInFunction1(op) => 0xB0 | (builtInOp1(op) << 8)
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
      
      case ArraySwap => 0x8A
      
      case DerefObject => 0xA0
      case DerefArray => 0xA1

      case BuiltInFunction2(op) => 0xB1 | (builtInOp2(op) << 8)
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

    def builtInOp1(op: BuiltInOp1) = op match {
      case GetMillis => 0x27
      case TimeZone => 0x00
      case Season => 0x13

      case Year => 0x01
      case QuarterOfYear => 0x02
      case MonthOfYear => 0x03
      case WeekOfYear => 0x04
      case WeekOfMonth => 0x14
      case DayOfYear => 0x10
      case DayOfMonth => 0x05
      case DayOfWeek => 0x06
      case HourOfDay => 0x07
      case MinuteOfHour => 0x08
      case SecondOfMinute => 0x09
      case MillisOfSecond => 0x11

      case Date => 0x15
      case YearMonth => 0x16
      case YearDayOfYear => 0x26
      case MonthDay => 0x17
      case DateHour => 0x18
      case DateHourMinute => 0x19
      case DateHourMinuteSecond => 0x20
      case DateHourMinuteSecondMillis => 0x21
      case TimeWithZone => 0x22
      case TimeWithoutZone => 0x23
      case HourMinute => 0x24
      case HourMinuteSecond => 0x25
    }

    def builtInOp2(op: BuiltInOp2) = op match {
      case ChangeTimeZone => 0x00
      case MillisToISO => 0x01

      case YearsBetween   => 0x02
      case MonthsBetween  => 0x03
      case WeeksBetween   => 0x04
      case DaysBetween    => 0x05
      case HoursBetween   => 0x06
      case MinutesBetween => 0x07
      case SecondsBetween => 0x08
      case MillisBetween  => 0x09
    }
    
    if (!stream.isEmpty) {
      val (opcode, pad, arg) = stream.head match {
        case Map1(op) => (0x00, 0.toShort, unaryOpNum(op))
        case Map2Match(op) => (0x01, 0.toShort, binaryOpNum(op))
        case Map2Cross(op) => (0x02, 0.toShort, binaryOpNum(op))
        case Map2CrossLeft(op) => (0x04, 0.toShort, binaryOpNum(op))
        case Map2CrossRight(op) => (0x06, 0.toShort, binaryOpNum(op))
        
        case Reduce(red) => (0x08, 0.toShort, reductionNum(red))
        
        case VUnion => (0x10, 0.toShort, 0)
        case VIntersect => (0x11, 0.toShort, 0)
        
        case IUnion => (0x12, 0.toShort, 0)
        case IIntersect => (0x13, 0.toShort, 0)
        
        case i @ FilterMatch(depth, Some(_)) => (0x14, depth, table(i))
        case i @ FilterCross(depth, Some(_)) => (0x16, depth, table(i))
        case i @ FilterCrossLeft(depth, Some(_)) => (0x17, depth, table(i))
        case i @ FilterCrossRight(depth, Some(_)) => (0x18, depth, table(i))
        
        case FilterMatch(depth, None) => (0x14, depth, 0)
        case FilterCross(depth, None) => (0x16, depth, 0)
        case FilterCrossLeft(depth, None) => (0x17, depth, 0)
        case FilterCrossRight(depth, None) => (0x18, depth, 0)
        
        case Split => (0x1A, 0.toShort, 0)
        case Merge => (0x1B, 0.toShort, 0)
        
        case Dup => (0x20, 0.toShort, 0)
        case i @ Swap(_) => (0x28, 0.toShort, table(i))
        
        case i @ Line(_, _) => (0x2A, 0.toShort, table(i))
        
        case LoadLocal(tpe) => (0x40, 0.toShort, typeNum(tpe))
        
        case i @ PushString(_) => (0x80, 0.toShort, table(i))
        case i @ PushNum(_) => (0x81, 0.toShort, table(i))
        case PushTrue => (0x82, 0.toShort, 0)
        case PushFalse => (0x83, 0.toShort, 0)
        case PushObject => (0x84, 0.toShort, 0)
        case PushArray => (0x85, 0.toShort, 0)
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
  
  private def writePredicate(pred: Predicate, buffer: ByteBuffer) = {
    writeInt(pred.length, buffer)
    writePredicateStream(pred, buffer, 4)
  }
  
  private[this] def writePredicateStream(stream: Vector[PredicateInstr], buffer: ByteBuffer, written: Int): Int = {
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
      writePredicateStream(stream.tail, buffer, written + 4)
    } else {
      written
    }
  }
  
  private def writeLineInfo(num: Int, text: String, buffer: ByteBuffer) = {
    writeInt(text.length * 2 + 4, buffer)
    buffer.putInt(num)
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
      case i @ FilterMatch(_, Some(_)) => i -> currentInt()
      case i @ FilterCross(_, Some(_)) => i -> currentInt()
      case i @ FilterCrossLeft(_, Some(_)) => i -> currentInt()
      case i @ FilterCrossRight(_, Some(_)) => i -> currentInt()
      
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
