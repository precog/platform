package com.querio.bytecode

import java.nio.ByteBuffer
import java.nio.BufferUnderflowException
import scala.annotation.tailrec

trait Reader extends Instructions {
  import Function._
  
  def read(buffer: ByteBuffer): Vector[Instruction] = {
    val version = buffer.getInt()
    
    val major = (version >> 24) & 0x7F
    val minor = (version >> 16) & 0xFF
    val release = version & 0xFFFF
    
    // TODO check version compatibility
    
    val table = readSymbolTable(buffer)
    readStream(buffer, table, Vector())
  }
  
  @tailrec
  private[this] def readStream(buffer: ByteBuffer, table: Map[Int, ByteBuffer], acc: Vector[Instruction]): Vector[Instruction] = {
    val opcode = try {
      Some(buffer.getLong())
    } catch {
      case _: BufferUnderflowException => None
    }
    
    val instr = opcode flatMap { code =>
      lazy val parameter = (code & 0xFFFFFFFF).toInt
      
      lazy val tableEntry = table get parameter
      
      lazy val predicate = tableEntry map { b => readPredicate(b, Vector()) }
      
      lazy val lineInfo = tableEntry flatMap readLineInfo
      
      lazy val string = tableEntry flatMap readString
      
      lazy val num = tableEntry flatMap readNum
      
      lazy val unOp = (code & 0xFF).toByte match {
        case 0x40 => Some(Comp)
        case 0x41 => Some(Neg)
        
        case 0x61 => Some(WrapArray)
        
        case _ => None
      }
      
      lazy val binOp = (code & 0xFF).toByte match {
        case 0x00 => Some(Add)
        case 0x01 => Some(Sub)
        case 0x02 => Some(Mul)
        case 0x03 => Some(Div)
        
        case 0x10 => Some(Lt)
        case 0x11 => Some(LtEq)
        case 0x12 => Some(Gt)
        case 0x13 => Some(GtEq)
        
        case 0x20 => Some(Eq)
        case 0x21 => Some(NotEq)
        
        case 0x30 => Some(Or)
        case 0x31 => Some(And)
        
        case 0x60 => Some(WrapObject)
        
        case 0x80 => Some(JoinObject)
        case 0x81 => Some(JoinArray)
        
        case 0xA0 => Some(DerefObject)
        case 0xA1 => Some(DerefArray)
        
        case _ => None
      }
      
      lazy val reduction = (code & 0xFF).toByte match {
        case 0x00 => Some(Count)
        
        case 0x01 => Some(Mean)
        case 0x02 => Some(Median)
        case 0x03 => Some(Mode)
        
        case 0x04 => Some(Max)
        case 0x05 => Some(Min)
        
        case 0x06 => Some(StdDev)
        case 0x07 => Some(Sum)
        
        case _ => None
      }
      
      lazy val tpe = (code & 0xFF).toByte match {
        case 0x00 => Some(Het)
      }
      
      lazy val depth = ((code >> 32) & 0xFFFFFF).toInt
      
      lazy val instruction = ((code >> 62) & 0xFF).toByte match {
        // operative instructions
        
        case 0x00 => unOp map Map1
        case 0x01 => binOp map Map2Match
        case 0x02 => binOp map Map2CrossLeft
        case 0x04 => binOp map Map2CrossRight
        case 0x06 => binOp map Map2Cross
        
        case 0x08 => reduction map Reduce
        
        case 0x10 => Some(VUnion)
        case 0x11 => Some(VIntersect)
        
        case 0x12 => Some(IUnion)
        case 0x13 => Some(IIntersect)
        
        case 0x14 => predicate map { p => FilterMatch(depth, p) }
        case 0x16 => predicate map { p => FilterCross(depth, p) }
        
        case 0x1A => Some(Split)
        case 0x1B => Some(Merge)
        
        // manipulative instructions
        
        case 0x20 => Some(Dup)
        case 0x21 => Some(Swap(parameter))
        
        case 0x2A => lineInfo map tupled(Line)
        
        // introductive instructions
        
        case 0x40 => tpe map LoadLocal
        
        case 0x80 => string map PushString
        case 0x81 => num map PushNum
        case 0x82 => Some(PushTrue)
        case 0x83 => Some(PushFalse)
        case 0x84 => Some(PushObject)
        case 0x85 => Some(PushArray)
        
        case _ => None
      }
      
      instruction
    }
    
    instr match {
      case Some(i) => readStream(buffer, table, acc :+ i)
      case None => acc
    }
  }
  
  @tailrec
  private[this] def readPredicate(buffer: ByteBuffer, acc: Vector[PredicateInstr]): Predicate = {
    val opcode = try {
      Some(buffer.get)
    } catch {
      case _: BufferUnderflowException => None
    }
    
    val instr = opcode collect {
      case 0x00 => Add
      case 0x01 => Sub
      case 0x02 => Mul
      case 0x03 => Div
      
      case 0x41 => Neg
      
      case 0x30 => Or
      case 0x31 => And
      
      case 0x40 => Comp
      
      case 0xA0 => DerefObject
      case 0xA1 => DerefArray
      
      case 0xFF => Range
    }
    
    instr match {
      case Some(i) => readPredicate(buffer, acc :+ i)
      case None => acc
    }
  }
  
  private def readLineInfo(buffer: ByteBuffer): Option[(Int, String)] = {
    try {
      val number = buffer.getInt
      readString(buffer) map { str => number -> str }
    } catch {
      case _ => None
    }
  }
  
  private def readString(buffer: ByteBuffer): Option[String] = {
    try {
      Some(buffer.asCharBuffer.toString)
    } catch {
      case _ => None
    }
  }
  
  private def readNum(buffer: ByteBuffer): Option[String] = readString(buffer)
  
  private def readSymbolTable(buffer: ByteBuffer): Map[Int, ByteBuffer] = {
    def loop(acc: Map[Int, ByteBuffer]): Map[Int, ByteBuffer] = {
      val id = buffer.getInt()
      if (id == 0) {
        acc
      } else {
        val len = buffer.getInt()
        loop(acc + (id -> getBuffer(buffer, len)))
      }
    }
    
    loop(Map())
  }
  
  private def getBuffer(buffer: ByteBuffer, len: Int): ByteBuffer = {
    val back = ByteBuffer.allocate(len)
    val arr = new Array[Byte](len)
    buffer.get(arr)
    back.put(arr)
    back
  }
}
